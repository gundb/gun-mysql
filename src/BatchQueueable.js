import Queueable from './queue/Queueable';

export default class BatchQueueable extends Queueable {
    constructor(cb, pool) {
        super(cb);
        this.pool = pool;
        this.connection = null;
    }
    prepare() {
        this.pool.getConnection((err, connection) => {
            this.connectErr = err;
            this.connection = connection;
            this.ready = !err;
            this.emit('ready');
        });
    }
    run(done) {
        if (this.connectErr || !this.connection) {
            done(this.connectErr);
        } else {
            this.cb(this.connection, err => {

                // Data is written! Release connection back into the pool.
                // This is critical otherwise the pool with become easily 
                // overwhelmed after only a few writes
                this.connection.release();
                done(err);
            });
        }
    }
    kill() {
        if (this.connection) {
            this.connection.destroy();
        }    
    }
    flush() {
        if (this.connection) {
            this.connection.destroy();
        }    
    }
}