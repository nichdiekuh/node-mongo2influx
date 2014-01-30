node-mongo2influx
=================

Migrates time series data from mongodb to influxdb.

It takes a mongodb and an influxdb database, reads *ALL* mongodb collections, passes each row to a "prepareData function"
and inserts the results into influxdb.


## Options

```js
var mongo2influx = new Mongo2Influx({
        influxdb : {
            user        : 'my-database-user',
            password    : 'f4nyp4ss',
            hostname    : 'xx.xx.xx.xx',
            database    : 'my-dest-database'
        },
        mongodb : {
            hostname    : 'xx.xx.xx.xx',
            database    : 'source-database'

        },
        logging     : true, // defaults to true, logs progress to cli
        limit       : 4, // number of parallel requests, defaults to 4
        emptySeries : false // if true, empties influx series before inserting new rows. defaults to false
    });


```

Take a look /examples/migrate.js for further instructions.


This repository is under development.

