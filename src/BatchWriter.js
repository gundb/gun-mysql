import ReactiveQueue from './queue/ReactiveQueue';
import Event from 'events';

export default class BatchWriter extends Event {
    constructor() {
        super();
        this.queues = {};
        return this;
    }
    batch(key, batch = [], done) {
        let queue = this._wireQueue(key || Date.now());
        queue.done = done;
        batch.forEach(update => {
            queue.queue.push(update);
        });
        return queue;
    }
    _wireQueue(key, queueable) {
        if (!this.queues[key]) {
            const queue = new ReactiveQueue();
            queue.on('empty', this._queueEmpty.bind(this, key));
            queue.on('item:done', res => {
                this.emit('item:done', {key, res});
            });
            queue.on('item:err', err => {
                this.emit('item:err', {key, err});
                queue.flush();
            });
            this.queues[key] = {queue}
        }
        return this.queues[key];
    }
    _queueEmpty(key) {
        this.emit('batch:finished', {key});
        if (this.queues[key] && this.queues[key].done) {
            this.queues[key].done();
        }
        delete this.queues[key];
    }
}
    