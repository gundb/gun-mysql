import {Flint, KeyValAdapter, Mixins} from './../../gun-flint';
import Connector from './Connector';

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

Flint.register(new KeyValAdapter({
    initialized: false,
    ready: false,
    mixins: [
        Mixins.ResultStreamMixin
    ],
    get: function(key, stream) {
        if (this.initialized) {
            if (!this.ready) {
                const get = this.get.bind(this, key, stream);
                setTimeout(get, 500);
            } else {
                const valStream = this.mysql.queryStream(`SELECT * FROM ${this._valTable()} WHERE` + '`key`' + `= '${key}';`);
                const relStream = this.mysql.queryStream(`SELECT * FROM ${this._relTable()} WHERE` + '`key`' + `= '${key}';`);
                
                let resultStream = row => {
                    stream.in(coerceResults(row));
                };

                Promise.all([
                    this._streamGetResults(valStream, resultStream),
                    this._streamGetResults(relStream, resultStream)
                ])
                .then(results => {
                    let isLost = (results && results.length === 2 && results[0] === this.errors.lost && results[1] === this.errors.lost)
                    stream.done(isLost ? this.errors.lost : null);
                })
                .catch(stream.done);
            }
        }
    },
    _streamGetResults(db, stream) {
        return new Promise((resolve, reject) => {
            let receivedResults = false;
            let returnErr = null;

            // Stream Results back to gun
            db
                .on('error', err => {
                    
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
    put: function(key, batch, done) {
        if (this.initialized) {
            if (!this.ready) {
                const put = this.put.bind(this, key, batch, done);
                setTimeout(put, 500);
            } else {
                const _this = this;
                const table = this.mysqlOptions.table || 'gun_mysql';

                const batchWriter = (function(bat, done) {
                    let write = {}
                    write.count = 0;
                    write.finished = bat.length;
                    write.done = done;

                    write.write = function(err) {
                        this.count++;
                        if (err) {
                            done(_this.errors.internal);
                        } else if (this.count === this.finished) {
                            done();
                        }
                    }.bind(write);
                    return write;
                })(batch, done);

                batch.forEach(node => {
                    const valStream = _this.mysql.queryStream([
                        `SELECT id FROM ${this._valTable()} WHERE `,
                        '`key` = \'', key, '\' AND ',
                        `nodeKey = '${node.key}';`
                    ].join(''));
                    const relStream = _this.mysql.queryStream([
                        `SELECT id FROM ${this._relTable()} WHERE `,
                        '`key` = \'', key, '\' AND ',
                        `nodeKey = '${node.key}';`
                    ].join(''));

                    let update = row => {
                        this._update(row.id, node, batchWriter);
                    }

                    Promise.all([
                        this._streamGetResults(valStream, update),
                        this._streamGetResults(relStream, update)
                    ])
                    // DB Stream results.
                    .then(results => {

                        // Neither tables had any data
                        if (results && results.length === 2 && results[0] === this.errors.lost && results[1] === this.errors.lost) {
                            this._putKeyVal(key, node, batchWriter);
                        }
                    })
                    // Pass Err to Write
                    .catch(batchWriter.write);
                });
            }
        }
    },
    _relTable: function() {
        return `${this.mysqlOptions.table}_rel`;
    },
    _valTable: function() {
        return `${this.mysqlOptions.table}_val`;
    },
    _update: function(id, node, batch) {
        if (node && Object.keys(node).indexOf('rel') !== -1) {
            this._updateRel(id, node, batch);
        } else {
            this._updateVal(id, node, batch);
        }
    },
    _updateVal: function(id, node, batch) {
        this.mysql.query(
            [
                `UPDATE ${this._valTable()} SET `,
                '`val` = ?, `state` = ?, `type` = ? ',
                'WHERE id = ? LIMIT 1;'
            ].join(''),
            [
                !isNil(node.val) ? node.val.toString() : '',
                node.state || 0,
                getTypeKey(node.val),
                id
            ],
            batch.write
        );
    },
    _updateRel: function(id, node, batch) {
        this.mysql.query(
            [
                `UPDATE ${this._relTable()} SET `,
                '`rel` = ?, `state` = ? ',
                'WHERE id = ? LIMIT 1;'
            ].join(''),
            [
                !isNil(node.rel) ? node.rel.toString() : '',
                node.state || 0,
                id
            ],
            batch.write
        );
    },
    _putKeyVal: function(key, node, batch) {
        if (node && Object.keys(node).indexOf('rel') !== -1) {
            this._putRel(key, node, batch);
        } else {
            this._putVal(key, node, batch);
        }
    },
    _putVal: function(key, node, batch) {

        // NEW KEY:VAL
        this.mysql.query(
            [
                `INSERT INTO ${this._valTable()} SET `,
                '`key` = ?, `nodeKey` = ?, `val` = ?, `state` = ?, `type` = ?;'
            ].join(''),
            [
                key,
                node.key,
                !isNil(node.val) ? node.val.toString() : '',
                node.state || 0,
                getTypeKey(node.val),
            ],
            batch.write
        );
    },
    _putRel: function(key, node, batch) {

        // NEW KEY:REL
        this.mysql.query(
            [
                `INSERT INTO ${this._relTable()} SET `,
                '`key` = ?, `nodeKey` = ?, `rel` = ?, `state` = ?;'
            ].join(''),
            [
                key,
                node.key,
                !isNil(node.rel) ? node.rel.toString() : '',
                node.state || 0,
            ],
            batch.write
        );
    },
    opt: function(context, opt, done) {
        let {mysql} = opt;
        if (mysql) {
            this.initialized = true;
            this.mysqlOptions = mysql;
            this.mysql = new Connector(mysql);
            this._tables()
        } else {
            this.initialized = false
        }
    },
    _tables: function() {
        Promise.all([
            this._createValTable(),
            this._createRelTable()
        ]).then(() => {
            this.ready = true;
        });
    },
    _createValTable: function() {
        return new Promise((resolve, reject) => {
            this.mysql.query(
                [
                    `CREATE TABLE IF NOT EXISTS ${this._valTable()} ( `,
                    '`id`      INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, ',
                    '`key`    VARCHAR(64) NOT NULL, ',
                    '`nodeKey` VARCHAR(64) NOT NULL, ',
                    '`state`   BIGINT, ',
                    '`val`     LONGTEXT, ',
                    '`type`    TINYINT, ',
                    'INDEX key_index (`key`, `nodeKey`)',
                    ') ENGINE=INNODB;'
                ].join(''),
                err => {
                    if (!err) {
                        resolve()
                    } else {
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
                    '`nodeKey` VARCHAR(64) NOT NULL, ',
                    '`state`   BIGINT, ',
                    '`rel`     TINYTEXT, ',
                    'INDEX key_index (`key`, `nodeKey`)',
                    ') ENGINE=INNODB;'
                ].join(''),
                err => {
                    if (!err) {
                        resolve()
                    } else {
                        reject();
                    }
                }
            );
        });
    }
}));