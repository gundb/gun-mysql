import Event from 'events';
import Queueable from './Queueable';

export default class ReactiveQueue extends Event {
    constructor() {
        super();
        this.queue = [];
        this.onDeck = null;

        this.on('item:added', run => {
            if (run) {
                this.run();
            }
        });

        this.on('item:done', () => {
            if (this.queue.length) {
                this.run();
            } else {
                this.emit('queue:empty')
            }
        });
    }
    push(item) {
        if (!(item instanceof Queueable)) {
            throw 'ReactiveQueue.push expects an instance that extends Queueable';
        }

        this.queue.push(item);
        this.emit('item:added', this.queue.length)
    }
    run() {

        if (!this.onDeck) {
            // Popoff first item
            this.onDeck = this.queue.shift();
            this.emit('item:ondeck', this.onDeck);

            // Handle Finished 
            this.onDeck.on('done', res => {
                this.onDeck = null;
                this.emit('item:done', res);
            });

            // Handler Error
            this.onDeck.on('error', err => {
                this.emit('item:err', err);
                if (this.onDeck.canRetry()) {
                    let requeue = this.onDeck;
                    setTimeout(() => {
                        this.emit('item:reqeue', requeue);
                        this.queue.push(requeue);
                    }, requeue.timeout);
                } else if (this.queue.length) {
                    this.run();
                }
                this.onDeck = null;
            });
            this.onDeck.runQueueable();
        }
    }
    flush(args) {
        this.queue = [];
        this.emit('queue:empty', args);
        if (this.onDeck) {
            this.onDeck.kill();
            this.onDeck = null;
        }
    }
}