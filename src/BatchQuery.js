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
        while(this.vals.length) {
            let vals = this.vals.shift();
            valStr += `('${vals[0]}', '${vals[1]}', '${vals[2]}', ${vals[3]}, ${vals[4]}, ${vals[5]}), `;
        }
        valStr = valStr.replace(/\,\s$/, ' ');
        return (this.query[0].replace('__VALS__', valStr) + ';');
    }

}