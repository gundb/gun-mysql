import Event from 'events';

export default class Queueable extends Event {
    constructor(cb, retries = 1, timeout = 1000) {
       super();
       this.cb = cb || function(done) { done(); };
       this.retries = retries;
       this.timeout = timeout;
       this.ready = false;
       this.attempt = 0;
    }
    increment() {
        this.attempt++;
    }
    runQueueable() {
       this.increment();

       if (this.ready) {
        this.__fire();
       } else {
           this.on('ready', this.__fire.bind(this));
       }
    }
    __fire() {
       try {
           this.run(res => {
               if (res instanceof Error) {
                   throw res;
               } else {
                 this.emit('done', res);
               }
           });
       } catch(err) {
            this.onFail(err);
       }
    }
    prepare() {
        this.ready = true;
        this.emit('ready');
    }
    run(done) {
        this.cb(done);
    }
    kill() {
        
    }
    flush() {
        
    }
    onFail(err) {
        this.emit('error', err);
    }
    canRetry() {
        return this.attempt < this.retries; 
    }
}