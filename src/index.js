import {Flint, KeyValAdapter} from 'gun-flint';
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
    results.forEach(result => {
        result.val = !isNil(result.val) ? coerce(result.type, result.val) : "";
    });
    return results;
}

function isNil(val) {
    return val === null || val === undefined;
}

Flint.register(new KeyValAdapter({
    initialized: false,
    ready: false,
    get: function(key, done) {
        if (this.initialized) {
            if (!this.ready) {
                const get = this.get.bind(this, key, done);
                setTimeout(get, 500);
            } else {
                this.mysql.query(`SELECT * FROM ${this.mysqlOptions.table} WHERE` + '`key`' + `= '${key}';`, (err, results, fields) => {
                    if (err) {
                        done(this.errors.internal);
                    } else {
                        let err = !results || results.length === 0 ? this.errors.lost : null;
                        done(err, coerceResults(results));
                    }
                });
            }
        }
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
                    _this.mysql.query([
                        `SELECT id FROM ${table} WHERE `,
                        '`key` = \'', key, '\' AND ',
                        `nodeKey = '${node.key}';`
                    ].join(''), function(err, results) {
                        if (err) {
                            batchWriter.write(err);
                        } else if (!results || results.length === 0) {

                            // NEW KEY:VAL
                            _this.mysql.query(
                                [
                                    `INSERT INTO ${table} SET `,
                                    '`key` = ?, `nodeKey` = ?, `rel` = ?, `val` = ?, `state` = ?, `type` = ?;'
                                ].join(''),
                                [
                                    key,
                                    node.key,
                                    node.rel || '',
                                    !isNil(node.val) ? node.val.toString() : '',
                                    node.state || 0,
                                    getTypeKey(node.val),
                                ],
                                batchWriter.write
                            );
                        } else {

                            // UPSERT
                            _this.mysql.query(
                                [
                                    `UPDATE ${table} SET `,
                                    '`rel` = ?, `val` = ?, `state` = ?, `type` = ? ',
                                    'WHERE id = ? LIMIT 1;'
                                ].join(''),
                                [
                                    node.rel || '',
                                    !isNil(node.val) ? node.val.toString() : '',
                                    node.state || 0,
                                    getTypeKey(node.val),
                                    results[0].id
                                ],
                                batchWriter.write
                            );
                        }
                    });
                });
            }
        }
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
        this.mysql.query(
            [
                `CREATE TABLE IF NOT EXISTS ${this.mysqlOptions.table} ( `,
                '`id`      INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, ',
                '`key`    VARCHAR(64) NOT NULL, ',
                '`nodeKey` VARCHAR(64) NOT NULL, ',
                '`state`   BIGINT, ',
                '`rel`     VARCHAR(64), ',
                '`val`     LONGTEXT, ',
                '`type`    TINYINT, ',
                'INDEX key_index (`key`, `nodeKey`)',
                ') ENGINE=INNODB;'
            ].join(''),
            (err, results) => {
                if (!err) {
                    this.ready = true;
                }
            }
        );
    }
}));