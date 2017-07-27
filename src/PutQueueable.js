import Queueable from './queue/Queueable';
import BatchQuery from './BatchQuery';

export default class PutQueueable extends Queueable {
    constructor(connection, tableName) {
        super();
        this.connection = connection;
        this._insertVal = new BatchQuery(`INSERT INTO ${tableName} (\`key\`, field, val, state, type, isRel) VALUES __VALS__`);
    }

    // Queueable Callback
    run(done) {
        this._runInsertVals()
        .then(() => done())
        .catch(err => done(err));
    }

    /**
     * All six parameters are required in order to add an insert
     * 
     * @public
     * @instance
     * 
     * @param {string} key   The node UUID / key
     * @param {string} field The field on the node
     * @param {mixed}  val   The value for field
     * @param {number} state Conflict resolution state
     * @param {number} type  Mapped to value type
     * @param {number} isRel Whether or not the value is a relations (as 0 or 1)
     */
    insertRow(...vals) {

        // Safe guard
        if (vals.length !== 6) {
            return;
        }

        this._insertVal.addVals(vals);
    }

    /**
     * Run the batch insert against the DB.
     * 
     * @private
     * @instance
     * 
     * @return Promise
     */
    _runInsertVals() {
        return new Promise((resolve, reject) => {
            if (!this._insertVal.getVals().length) {
                resolve();
            } else {
                this.connection.query(this._insertVal.getQuery(), err => {
                    if (err) {
                        reject();
                    } else {
                        resolve();
                    }
                });
            }
        });
    }
}