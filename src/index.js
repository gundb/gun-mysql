import {Flint, KeyValAdapter, Mixins} from './../../gun-flint';
import ReactiveQueue from './queue/ReactiveQueue';
import Queueable from './queue/Queueable';
import BatchWriter from './BatchWriter';
import PutQueueable from './PutQueueable';
import Connector from './Connector';
import Query from './Query';

const type = {
    number: 0,
    boolean: 1,
    string: 2,
    undefined: 3,
    null: 4
};

function getTypeKey(val) {
    let typeKey = type.string;
    switch(true) {
        case (val === null):
            typeKey = type.null;
            break;
        case (val === undefined):
            typeKey = type.undefined;
            break;
        case (typeof val === 'string'):
            typeKey = type.string;
            break;
        case (typeof val === 'boolean'):
            typeKey = type.boolean;
            break;
        case (typeof val === 'number'): 
            typeKey = type.number;
            break;
    }
    return typeKey;
}

function coerce(typeKey, val) {
    var coerced = val;
    switch(typeKey) {
        case type.number: 
            coerced = Number(val);
            break;
        case type.boolean:
            coerced = val === "true";
            break;
        case type.undefined:
            coerced = undefined;
            break;
        case type.null:
            coerced = null;
            break;
    }
    return coerced;
}

function coerceResults(results = []) {
    if (typeof results === 'array') {
        results.forEach(result => {
            result.val = !isNil(result.val) ? coerce(result.type, result.val) : "";
        });
    } else {
        results.val = !isNil(results.val) ? coerce(results.type, results.val) : "";
    }
    return results;
}

function isNil(val) {
    return val === null || val === undefined;
}

function makeBatchWriter(batch, errors, logger, done) {
    let write = {}
    write.count = 0;
    write.finished = batch.length;
    write.done = done;

    write.write = function(err) {
        this.count++;
        if (err) {
            // Log
            logger("Errored writing data", err);

            // Send internal response
            done(errors.internal);
        } else if (this.count === this.finished) {
            done();
        }
    }.bind(write);
    return write;
};


Flint.register(new KeyValAdapter({
    initialized: false,
    ready: false,
    unableToProceed: false,
    batchWriter: new BatchWriter(),
    queue: new ReactiveQueue(),
    mixins: [
        Mixins.ResultStreamMixin
    ],
    opt: function(context, opt, initial) {
        let {mysql} = opt;
        if (initial) {
            if (mysql) {
                this.initialized = true;
                this.mysqlOptions = mysql;
                
                // Check Table Prefix for safety (only W chars). Otherwise, this opens
                // the possibility for SQL injection since the tablename cannot be
                // a bound param in prepared statements.
                this.mysqlOptions.tablePrefix = this.mysqlOptions.tablePrefix || 'gun_mysql';
                if (this.mysqlOptions.tablePrefix && /[^a-zA-z_]/.test(this.mysqlOptions.tablePrefix)) {
                    throw `The table option can only contain letters and '_'; The value '${this.mysqlOptions.tablePrefix}' is invalid. Aborting.`;
                }

                // Logging
                this.logger = this.mysqlOptions.logger || console;

                // Continue
                this.mysql = new Connector(mysql);
                this._tables()
            } else {
                this.initialized = false;
            }
        }
    },
    get: function(key, field, stream) {
        if (this.initialized) {
            if (!this.ready) {
                const get = this.get.bind(this, key, field, stream);
                setTimeout(get, 500);
            } else {

                // Prepare Relationship and Value Queries
                let valQuery = new Query(`SELECT * FROM ${this._valTable()} WHERE \`key\` = ?`, [key]);
                let relQuery = new Query(`SELECT * FROM ${this._relTable()} WHERE \`key\` = ?`, [key]);

                // If field specific, add field search delimiter
                if (field) {
                    const addition = 'AND `field` = ?';
                    valQuery.clause(addition, field);
                    relQuery.clause(addition, field);
                }

                // Create Streams
                const valStream = this.mysql.queryStream(valQuery.getQuery(), valQuery.getBoundVars());
                const relStream = this.mysql.queryStream(relQuery.getQuery(), valQuery.getBoundVars());
                
                // Result found callback
                let resultStream = row => {
                    stream.in(coerceResults(row));
                };

                // Promisify Stream
                Promise.all([
                    this._streamGetResults(valStream, resultStream),
                    this._streamGetResults(relStream, resultStream)
                ])
                .then(promiseResult => {
                    let isLost = (promiseResult && promiseResult.length === 2 && promiseResult[0] === this.errors.lost && promiseResult[1] === this.errors.lost)
                    stream.done(isLost ? this.errors.lost : null);
                })
                .catch(stream.done);
            }
        }
    },
    put: function(batch, done) {
        if (this.initialized) {
            if (this.unableToProceed) {
                return;
            } else if (!this.ready) {
                const put = this.put.bind(this, batch, done);
                setTimeout(put, 500);
            } else {

                // Safeguard
                if (!batch || !batch.length) {
                    done();
                    return;
                }

                this._doPut(batch, done);   
            }
        }
    },
    _runPutTransaction(connection, batch, done) {

        // Prepare Rollback callback if necessary
        let rollback = err => {
            connection.rollback(() => {
                done(err);
            });
        }

        // Get a unique batch key using first Node key and Date
        let key = batch[0].key + '_' + Date.now();

        // Begin Batch Transaction
        let _this = this;
        var queue = [];
        connection.beginTransaction(err => {
            if (err) {
                rollback(err);
            } else {
            
                batch.forEach(node => {

                    function put(jobDone) {

                        // Prepare a job finished callback
                        let writeDone = err => {
                            if (err) {
                                throw err;
                            }
                            jobDone();
                        }

                        // Duing a put transaction, first row for any matching key + field pairs
                        const valStream = _this.mysql.queryStream([
                            `SELECT id FROM ${_this._valTable()} WHERE `,
                            '`key` = \'', node.key, '\' AND ',
                            `field = '${node.field}' LIMIT 1;`
                        ].join(''));
                        const relStream = _this.mysql.queryStream([
                            `SELECT id FROM ${_this._relTable()} WHERE `,
                            '`key` = \'', node.key, '\' AND ',
                            `field = '${node.field}' LIMIT 1;`
                        ].join(''));

                        // Prepare an update callback. Get results are streamed straight
                        // into the update callback.
                        let update = row => {
                            _this._update(connection, row.id, node, writeDone);
                        }

                        // Stream Get results, passing them to update as they are returned.
                        Promise.all([
                            _this._streamGetResults(valStream, update),
                            _this._streamGetResults(relStream, update)
                        ])
                        // DB Stream results.
                        .then(results => {

                            // Neither tables had any data
                            let errors = _this.errors;
                            if (results && results.length === 2 && results[0] === errors.lost && results[1] === errors.lost) {
                                _this._putKeyVal(connection, node.key, node, writeDone);
                            }
                        })
                        // Pass Err to Write
                        .catch(err => {
                            _this.logger.error("Error retrieving results for batch update", err)
                            throw err;
                        });
                    }

                    // Add put request to queue
                    queue.push(new PutQueueable(put));
                });

                // Queue up batch
                const writeBatch = this.batchWriter.batch(key, queue);

                // If any one item fails, rollback the entire sequence
                // and flush out the queue. The error bubble back up to Gun.
                writeBatch.queue.on('item:err', err => {
                    writeBatch.queue.flush(err);
                });

                // Once the queue is empty, we the batch has been
                // accepted without error. Commit the transaction.
                // If an error is received, the queue was flushed
                // prematurely due to a job error.
                writeBatch.queue.on('queue:empty', flushErr => {
                    if (flushErr) {
                        rollback(err);
                    } else {
                        connection.commit(err => {
                            if (err) {
                                rollback(err);
                            } else {
                                // SUCCESS!
                                done();
                            }
                        });
                    }
                });
            }
        });
    },

    /**
     * TODO: Make the Queue smarter to only delay writes for nodes that
     *       are currently in the queue for writing. Otherwise, apply them 
     *       immediately.
     * 
     * @param {array}    batch   An array of key:val nodes to update
     * @param {function} done    A callback to call once batch has been written
     */
    _doPut(batch = [], done) {

        // Retrieve a connection from the pool to use for the transaction
        this.mysql.getConnection().then(connection => {

            // Build a
            let call = jobDone => {
                this._runPutTransaction(connection, batch, (err) => {
                    if (err) {

                        // Log the err
                        this.logger.log(err);

                        // Tell Flint the job returned an internal error
                        done(this.errors.internal);

                        // Tell the queue the job failed
                        throw err;
                    } else {

                        // Tell the queue that the job is finished
                        jobDone();

                        // Tell Gun that all is well
                        done(null);
                    }
                });
            };

            // Queue up a batch write process; this keeps all
            // acknowledgements groups.
            this.queue.push(new Queueable(call));
        })
        .catch(err => {
            this.logger.error("Failed to retrieve connection to write key:val batch updates", err);
            done(err);
        });
    },
    _streamGetResults(db, stream) {
        return new Promise((resolve, reject) => {
            let receivedResults = false;
            let returnErr = null;

            // Stream Results back to gun
            db
                .on('error', err => {
                    
                    // Log
                    this.logger.error("Errored retrieving results", err);

                    // Catch the err, retun on `end` event
                    returnErr = this.errors.internal;
                })
                .on('result', row => {
                    receivedResults = true;

                    // Coerce and send back
                    stream(row);
                })
                .on('end', () => {
                    
                    // Stream returned an error at some point. send internal err
                    if (returnErr) {
                        reject(returnErr);

                    // No results found before end event; send 404
                    } else if (!receivedResults) {
                        resolve(this.errors.lost);
                    } else {
                        resolve();
                    }
                });
        });
    },
    _relTable: function() {
        return `${this.mysqlOptions.tablePrefix}_rel`;
    },
    _valTable: function() {
        return `${this.mysqlOptions.tablePrefix}_val`;
    },
    _update: function(connection, id, node, batch) {
        if (node && Object.keys(node).indexOf('rel') !== -1) {
            this._updateRel(connection, id, node, batch);
        } else {
            this._updateVal(connection, id, node, batch);
        }
    },
    _updateVal: function(connection, id, node, write) {

        // Prepare Query
        let val = !isNil(node.val) ? node.val.toString() : '';
        const update = new Query(`UPDATE ${this._valTable()}`);
        update.clause('SET `val` = ?, `state` = ?, `type` = ? ', val, node.state || '', getTypeKey(node.val));
        update.clause('WHERE id = ? LIMIT 1', id);

        // Apply
        connection.query(update.getQuery(), update.getBoundVars(), write);
    },
    _updateRel: function(connection, id, node, write) {

        // Prepare Query
        let rel = !isNil(node.rel) ? node.rel.toString() : '';
        const update = new Query(`UPDATE ${this._relTable()}`);
        update.clause('SET `rel` = ?, `state` = ? ', rel, node.state || '');
        update.clause('WHERE id = ? LIMIT 1', id);

        // Apply
        connection.query(update.getQuery(), update.getBoundVars(), write);
    },
    _putKeyVal: function(connection, key, node, write) {
        if (node && Object.keys(node).indexOf('rel') !== -1) {
            this._putRel(connection, key, node, write);
        } else {
            this._putVal(connection, key, node, write);
        }
    },
    _putVal: function(connection, key, node, write) {

        // NEW KEY:VAL
        connection.query(
            [
                `INSERT INTO ${this._valTable()} SET `,
                '`key` = ?, `field` = ?, `val` = ?, `state` = ?, `type` = ?;'
            ].join(''),
            [
                key,
                node.field,
                !isNil(node.val) ? node.val.toString() : '',
                node.state || 0,
                getTypeKey(node.val),
            ],
            write
        );
    },
    _putRel: function(connection, key, node, write) {

        // NEW KEY:REL
        connection.query(
            [
                `INSERT INTO ${this._relTable()} SET `,
                '`key` = ?, `field` = ?, `rel` = ?, `state` = ?;'
            ].join(''),
            [
                key,
                node.field,
                !isNil(node.rel) ? node.rel.toString() : '',
                node.state || 0,
            ],
            write
        );
    },
    _tables: function() {
        Promise.all([
            this._createValTable(),
            this._createRelTable()
        ]).then(() => {
            this.ready = true;
        }).catch(err => {
            this.unableToProceed = true;
        });
    },
    _createValTable: function() {
        return new Promise((resolve, reject) => {
            this.mysql.query(
                [
                    `CREATE TABLE IF NOT EXISTS ${this._valTable()} ( `,
                    '`id`      INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, ',
                    '`key`    VARCHAR(64) NOT NULL, ',
                    '`field` VARCHAR(64) NOT NULL, ',
                    '`state`   BIGINT, ',
                    '`val`     LONGTEXT, ',
                    '`type`    TINYINT, ',
                    'INDEX key_index (`key`, `field`)',
                    ') ENGINE=INNODB;'
                ].join(''),
                err => {
                    if (!err) {
                        resolve()
                    } else {
                        this.logger.error("Errored creating value table. Unable to initialize Gun-MySQL adapter.", err);
                        reject();
                    }
                }
            );
        });
    },
    _createRelTable: function() {
        return new Promise((resolve, reject) => {
            this.mysql.query(
                [
                    `CREATE TABLE IF NOT EXISTS ${this._relTable()} ( `,
                    '`id`      INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, ',
                    '`key`    VARCHAR(64) NOT NULL, ',
                    '`field` VARCHAR(64) NOT NULL, ',
                    '`state`   BIGINT, ',
                    '`rel`     TINYTEXT, ',
                    'INDEX key_index (`key`, `field`)',
                    ') ENGINE=INNODB;'
                ].join(''),
                err => {
                    if (!err) {
                        resolve()
                    } else {
                        this.logger.error("Errored creating relationship table. Unable to initialize Gun-MySQL adapter.", err);
                        reject();
                    }
                }
            );
        });
    }
}));