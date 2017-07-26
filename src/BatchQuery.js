import Query from './Query';

export default class BatchQuery extends Query {

    addVals(...vals) {
        this.vals.push.apply(this.vals, vals);
    }

    getVals() {
        return this.vals;
    }

    getQuery() {
        let valStr = '';

        function join(vals) {
            let retVal = '';
            while (vals && vals.length) {
                let val = vals.shift();
                if (typeof val === 'string') {
                    val = val.replace(/'/, "\\'");
                    retVal = retVal + "'" + val + (vals.length ? "', " : "");
                } else {
                     retVal = retVal + ('' + val) + (vals.length ? ", " : "");
                }
            }
            return retVal;
        }

        while(this.vals.length) {
            let vals = this.vals.shift();
            valStr += `(${join(vals)}), `;
        }
        valStr = valStr.replace(/,\s*$/, ' ');
        return (this.query[0].replace('__VALS__', valStr) + ';');
    }

}