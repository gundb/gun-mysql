import {Flint, KeyValAdapter, Mixins} from 'gun-flint';
import ReactiveQueue from './queue/ReactiveQueue';
import Queueable from './queue/Queueable';
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
    if (typeof results !== 'array') {
        results = [results];
    }
    
    results.forEach(result => {
        if (result.isRel) {
            result.rel = result.val;
            delete result.val;
        } else {
            result.val = !isNil(result.val) ? coerce(result.type, result.val) : "";
        }
    });
    return results;
}

function isRelNode(node) {
    return node && Object.keys(node).indexOf('rel') !== -1;
}

function isNil(val) {
    return val === null || val === undefined;
}


Flint.register(new KeyValAdapter({
    initialized: false,
    ready: false,
    unableToProceed: false,
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

                // If field specific, add field search delimiter
                if (field) {
                    const addition = 'AND `field` = ?';
                    valQuery.clause(addition, field);
                }

                // Create Streams
                const valStream = this.mysql.queryStream(valQuery.getQuery(), valQuery.getBoundVars());
                
                // Result found callback
                let resultStream = row => {
                    stream.in(coerceResults(row));
                };

                // Promisify Stream
                this._streamGetResults(valStream, resultStream)
                .then(promiseResult => {
                    let isLost = (promiseResult && promiseResult === this.errors.lost)
                    stream.done(isLost ? this.errors.lost : null);
                })
                .catch(stream.done);
            }
        }
    },
    
    /**
     * @public
     * 
     * @param {array}    batch   The batch of writes to write
     * @param {function} done    Ack callback after write succeeds
     */
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
    
    /**
     * TODO: Make the Queue smarter to only delay writes for nodes that
     *       are currently in the queue for writing. Otherwise, apply them 
     *       immediately.
     * 
     * @param {array}    batch   An array of key:val nodes to update
     * @param {function} done    A callback to call once batch has been written
     */
    _doPut(batch = [], done) {

        // The design intent here is to have a queue of queues.
        // `this.queue` holds the batch queue. Each batch queue
        // is comprised of queued updates that are applied to the DB
        this.queue.push(new Queueable(jobDone => {
            this._putBatch(batch, err => {
                if (err) {
                    done(this.errors.internal);
                    jobDone(err);
                } else {
                    done();
                    jobDone();
                }
            });
        }));
    },

    /**
     * Run a batch update
     * 
     * @param {array}    batch   An array of key:val nodes to update
     * @param {function} done    A callback to call once batch has been written
     */
    _putBatch(batch, jobDone) {

        // First retreive a connection from the connection pool
        // In order to start a tranaction. This connection will be
        // used for the entire batch of writes/updates
        this.mysql.getConnection().then(connection => {
            this._runPutTransaction(connection, batch, err => {

                // Data is written! Release connection back into the pool.
                // This is critical otherwise the pool with become easily 
                // overwhelmed after only a few writes
                connection.release();
                
                if (err) {

                    // Log the err
                    this.logger.log(err);

                    // Tell the queue the job failed
                    jobDone(err);
                } else {

                    // Tell the queue that the job is finished
                    jobDone();
                }
            });
        })
        .catch(err => {
            this.logger.error("Failed to retrieve connection to write key:val batch updates", err);
            jobDone(err);
        });
    },

    /**
     * Given a connection, create the update transaction
     * 
     * @param {mysql.connection} connection  A DB connection which provides context for transaction
     * @param {array}    batch   An array of key:val nodes to update
     * @param {function} done    A callback to call once batch has been written
     */
    _runPutTransaction(connection, batch, done) {

        // Prepare Rollback callback if necessary
        let rollback = err => {
            connection.rollback(() => {
                done(err);
            });
        }

        // Begin Batch Transaction
        let _this = this;
        connection.beginTransaction(err => {
            if (err) {
                rollback(err);
            } else {
            
                // This queue will contain all of the writes/updates for the key:vals
                let queue = new ReactiveQueue();
                const insertJob = new PutQueueable(connection, this._valTable());

                // In terms of queue operations, the batch should write all updates
                // first as single queries; inserts will be applied in a single batch
                // query. The entirety of this is wrapped in a transaction. Once the 
                // transaction succeeds, calling `done` will remove this batch from the
                // write queue.
                let handled = 0;
                let insertsQueued = false;
                function enqueueInsertWhenReady(...insertVals) {

                    // Increment counter.
                    handled++;

                    // If we're doing an insert, add those to the insert query
                    // If insertVals is undefined, we can assume the insert was 
                    // handled via update of an existing value.
                    if (insertVals && insertVals.length) {
                        insertJob.insertRow.apply(insertJob, insertVals);
                    }
                    
                    // Have all the updates been handled? If so, push the insert
                    // job onto the queue as the final step.
                    if (handled === batch.length) {
                        insertsQueued = true;
                        queue.push(insertJob);
                    }
                }

                // The batch is an array of nodes to write to the server
                batch.forEach(node => {

                    // Retrieve queueable callback
                    let put = this._getPutRunner(connection, node, enqueueInsertWhenReady);

                    // Add put request to queue
                    queue.push(new Queueable(put));
                });

                // If any one item fails, rollback the entire sequence
                // and flush out the queue. The error should bubble back up to Gun.
                queue.on('item:err', err => {
                    queue.flush(err);
                });

                // Once the queue is empty, we the batch has been
                // accepted without error. Commit the transaction.
                // If an error is received, the queue was flushed
                // prematurely due to a job error.
                queue.on('queue:empty', flushErr => {

                    // The batch errored somewhere along the line
                    // Flush the queue and above transaction
                    if (flushErr) {
                        rollback(err);
                    
                    // If the entire batch hasn't been handled
                    // then wait until final inserts come in 
                    } else if (insertsQueued) {
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

    _getPutRunner(connection, node, enqueueInsertWhenReady) {
        
        /**
         *  This method is passed into the Queueable job.
         * 
         * @param {function} jobDone   Call after the job has completed
         */
        var _this = this;
        return function put(jobDone) {

            // Stream Get results, passing them to update as they are returned.
            _this._streamGetResults(_this._streamNodeVal(node), row => {

                enqueueInsertWhenReady();

                if (node.val !== row.val) {
                    _this._update(connection, row.id, node, err => {
                        jobDone(err);
                    });
                
                // Nothing to update. Tell queue this job is finished
                } else {
                    jobDone();
                }
            })
            // DB Stream results.
            .then(result => {

                // No data yet exists for this node key:val
                if (result && result === _this.errors.lost) {
                    let isRel = isRelNode(node) ? 1 : 0;
                    let val = isRel ? node.rel : node.val;
                    enqueueInsertWhenReady(node.key, node.field, !isNil(val) ? val.toString() : '', node.state || 0, getTypeKey(val), isRel);

                    // this batch element has finished. The actual insert
                    // will take place when the PutQueueable runs.
                    jobDone();
                }
            })
            // Pass Err to Write
            .catch(err => {
                _this.logger.error("Error retrieving results for batch update", err)
                 jobDone(err);
            });
        }
    },

    /**
     * Retrieve a pool connection that will stream get results back
     * 
     * @param {object} node The node to retrieve a value for
     */
    _streamNodeVal(node) {
        return this.mysql.queryStream([
            `SELECT id, val FROM ${this._valTable()} WHERE `,
            '`key` = \'', node.key, '\' AND ',
            `field = '${node.field}' LIMIT 1;`
        ].join(''));
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
    _valTable: function() {
        return `${this.mysqlOptions.tablePrefix}_val`;
    },
    _update: function(connection, id, node, write) {

        // Prepare Query
        let val = !isNil(node.val) ? node.val.toString() : '';
        const update = new Query(`UPDATE ${this._valTable()}`);
        update.clause('SET `val` = ?, `state` = ?, `type` = ?, `isRel` = ? ', val, node.state || '', getTypeKey(node.val), isRelNode(node) ? 1 : 0);
        update.clause('WHERE id = ? LIMIT 1', id);

        // Apply
        connection.query(update.getQuery(), update.getBoundVars(), write);
    },
    _tables: function() {
        this._createValTable()
        .then(() => {
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
                    '`isRel`   TINYINT(1), ',
                    '`type`    TINYINT(1), ',
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
    }
}));