import Queueable from './queue/Queueable';
import BatchQuery from './BatchQuery';

export default class PutQueueable extends Queueable {
    constructor(connection, context) {
        super();
        this.connection = connection;

        // Init Queries
        // 1. Update Value
        this._updateVal = new BatchQuery(`INSERT INTO ${context._valTable()} (id, val, state, type) VALUES __VALS__ ON DUPLICATE KEY UPDATE id=VALUES(id)`);

        // 2. Insert Value
        this._insertVal = new BatchQuery(`INSERT INTO ${context._valTable()} (\`key\`, field, val, state, type) VALUES __VALS__`);

        // 3. Update Relationship
        this._updateRel = new BatchQuery(`INSERT INTO ${context._relTable()} (id, rel, state) VALUES __VALS__ ON DUPLICATE KEY UPDATE id=VALUES(id)`);

        // 4. Insert Relationship
        this._insertRel = new BatchQuery(`INSERT INTO ${context._relTable()} (\`key\`, field, rel, state) VALUES __VALS__`);

    }

    // Queueable Callback
    run(done) {
        Promise.all([
            //this._runUpdateVals(),
            this._runInsertVals(),
            //this._runUpdateRels(),
            this._runInsertRels()
        ])
        .then(() => done())
        .catch(err => done(err));
    }
    // updateVal(val) {
    //     this._updateVal.addVals(val);
    // }
    insertVal(...vals) {
        this._insertVal.addVals(vals);
    }
    // updateRel(val) {
    //     this._updateRel.addVals(val);
    // }
    insertRel(...vals) {
        this._insertRel.addVals(vals);
    }
    _runQuery(query, resolve, reject) {
        if (!query.getVals().length) {
            resolve()
        } else {
            this.connection.query(query.getQuery(), err => {
                if (err) {
                    reject();
                } else {
                    resolve();
                }
            });
        }
    }
    // _runUpdateVals() {
    //     return new Promise((resolve, reject) => {
    //         this._runQuery(this._updateVal, resolve, reject);
    //     });
    // }
    _runInsertVals() {
        return new Promise((resolve, reject) => {
            this._runQuery(this._insertVal, resolve, reject);
        });
    }
    // _runUpdateRels() {
    //     return new Promise((resolve, reject) => {
    //         this._runQuery(this._updateRel, resolve, reject);
    //     });
    // }
    _runInsertRels() {
        return new Promise((resolve, reject) => {
            this._runQuery(this._insertRel, resolve, reject);
        });
    }
}