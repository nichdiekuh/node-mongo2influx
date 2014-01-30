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
        logging : true // default to true, logs progress to cli
    });


```

Take a look /examples/migrate.js for further instructions.


This repository is under development.


## usage

Best
