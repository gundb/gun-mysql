# Gun-MySQL

A MySQL adapter for GunJS. 

# WARNING: THIS IS NOT READY FOR PRODUCTION USE!! API unstable. Not fully tested.

Feel free to contribute and help get it ready for general use. :)

# Installation

Before installing, ensure that you have created a database and given a database user access to read/write from that database, as well as create tables.

`yarn add gun-mysql` or `npm install gun-mysql`.

```javascript

const Gun = require('gun');

// Must be added after Gun but before instantiating Gun
require('gun-mysql');

// Instantiate Gun
const gun = new Gun({
    file: false,
    server: httpServer,

    // The following are defaults. You can supply `true` to use all defaults
    mysql: {
        host: 'localhost',
        port: '3306',
        user: 'root',
        password: '',
        database: 'gun_mysql',
        table: 'gun_mysql'
    }
});
```

You should start to see the table fill up with data from your Gun database!