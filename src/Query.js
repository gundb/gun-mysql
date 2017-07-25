export default class Query {
    constructor(query = '', vals = []) {
        this.query = [query];
        this.vals = vals;
    }

    clause(clause, ...vals) {
        this.query.push(' ', clause);
        this.vals.push.apply(this.vals, vals);
    }

    getQuery() {
        return (this.query.join('') + ';');
    }

    getBoundVars() {
        return this.vals;
    }
}