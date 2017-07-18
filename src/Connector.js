import MySQL from 'mysql';

export default class Connector {
    constructor(options) {
        this.opt = options;

        if (!this.opt) {
            throw "No options passed to the adapter!";
        }

        this.pool = MySQL.createPool({
            connectionLimit : 10,
            host     : this.opt.host || 'localhost',
            user     : this.opt.user || 'root',
            password : this.opt.password || '',
            database : this.opt.database || 'gun_mysql'
        });

        this.pool.getConnection(function(err, connection) {
            console.log(err, connection);
        });
    }

    query(sql, vals, done) {
        if (typeof vals === 'function') {
            done = vals;
            this.pool.query(sql, done)
        } else {
            this.pool.query(sql, vals, done);
        }
    }
}
