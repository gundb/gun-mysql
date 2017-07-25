import Event from 'events';

export default class Queueable extends Event {
    constructor(cb, retries = 1, timeout = 1000) {
       super();
       this.cb = cb || function(done) { done(); };
       this.retries = retries;
       this.timeout = timeout;
       this.attempt = 0;
    }
    increment() {
        this.attempt++;
    }
    runQueueable() {
       this.increment();
       try {
           this.run(res => {
              this.emit('done', res);
           });
       } catch(err) {
            this.onFail(err);
       } 
    }
    run(done) {
        this.cb(done);
    }
    kill() {
        
    }
    onFail(err) {
        this.emit('error', err);
    }
    canRetry() {
        return this.attempt < this.retries; 
    }
}