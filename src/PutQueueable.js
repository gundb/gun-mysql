import Queueable from './queue/Queueable';

export default class PutQueueable extends Queueable {
    constructor(cb) {
        super();
        this.cb = cb;
    }
    run(done) {
        this.cb(done);
    }
}