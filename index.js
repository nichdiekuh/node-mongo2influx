

var _ = require('underscore');
var async = require('async');
var influx  = require('influx');

var MongoClient = require('mongodb').MongoClient
    , Server = require('mongodb').Server;

var configuration =
{
    influxserver : {
        user        : 'root',
        password    : 'root',
        hostname    : 'localhost',
        port        : 8086
    },
    influxdb : {
        user        : 'dbuser',
        password    : '',
        hostname    : 'localhost',
        port        : 8086,
        database    : 'default'

    },
    mongodb : {
        user        : '',
        password    : '',
        hostname    : 'localhost',
        port        : 27017,
        database    : 'default'
    },
    logging     : true,
    limit       : 4,
    emptySeries : false

}

var influxServer, influxDB, mongoClient, mongodb;


var Mongo2Influx = function(options)
{

    if (options.influxdb)
        _.extend(configuration.influxdb,options.influxdb);

    if (options.influxserver)
        _.extend(configuration.influxserver,options.influxserver);


    if (options.mongodb)
        _.extend(configuration.mongodb,options.mongodb);

    if (options.limit) configuration.limit = options.limit;
    if (options.logging) configuration.logging = options.logging;

}

Mongo2Influx.prototype.connect = function (cb)
{

    influxServer = influx(
        configuration.influxserver.hostname,
        configuration.influxserver.port,
        configuration.influxserver.user,
        configuration.influxserver.password,
        configuration.influxserver.database
    );

    influxDB = influx(
        configuration.influxdb.hostname,
        configuration.influxdb.port,
        configuration.influxdb.user,
        configuration.influxdb.password,
        configuration.influxdb.database
    );
    mongoClient = new MongoClient(new Server(configuration.mongodb.hostname, configuration.mongodb.port));

    mongoClient.open(function ( err, mongoClient ) {
        if (err) {
            return cb(err);
        }
        mongodb = mongoClient.db(configuration.mongodb.database);
        if (cb) cb(null);
    });
}


Mongo2Influx.prototype.log = function ()
{
    if (configuration.logging) {
        console.log(_.values(arguments).join(' '));
    }
}

Mongo2Influx.prototype.migrateCollection = function(prepareFunction, collection,callbackCollections)
{
    var self = this;
    var collectionName = collection.collectionName;
    if ( -1 !== collectionName.indexOf('system')) return callbackCollections();
    self.log('next collection: ',collectionName);
    var startDump = new Date();
    collection.find().toArray(function(err, results) {
        if (!err && _.isArray(results))
        {
            self.log('reading results from',collectionName,results.length,'rows, took',(new Date()-startDump),'ms');

            var index =0;
            var startMigration = new Date();
            async.eachLimit(results,configuration.limit,function(row,cb){
                var data = prepareFunction(row);

                if (!row.time) {
                    self.log('skipped row',row);
                    return cb();
                }
                influxDB.writePoint(collectionName, data , {pool : false}, function(err) {
                    if (err)
                    {
                        return cb(err)
                    }
                    else {
                        index++;
                        if (0 == index%1000)
                        {
                            var diff = (new Date()-startMigration) / 1000;
                            var ips = Math.round(1000 / diff);
                            self.log('collection',collectionName,'item #',index,'@',ips,'inserts/sec');
                            startMigration = new Date();
                        }
                        return cb();
                    }
                });
            },function(err)
            {
                if (err)
                {
                    self.log('error migrating collection',collectionName);

                } else {
                    self.log('collection done',collectionName);
                }
                callbackCollections(err);
            });
        } else {
            self.log('error',err);
            callbackCollections(err);
        }
    });
}


Mongo2Influx.prototype.migrateCollections = function( prepareFunction, options, collections, callback )
{
    var self = this;
    self.log('found',collections.length,'collection');
    async.eachSeries(collections,function( collection, callbackCollections )
    {
        if (true === configuration.emptySeries)
        {
            self.emptySeries(collection,function()
            {
                self.migrateCollection(prepareFunction, collection,callbackCollections);
            })
        } else {
            self.migrateCollection(prepareFunction, collection,callbackCollections);
        }
    },callback);


}



Mongo2Influx.prototype.emptySeries = function(collectionName,callback)
{
    influxDB.readPoints('DELETE FROM '+collectionName+' WHERE time < now();',callback);
}



Mongo2Influx.prototype.migrate = function ( prepareFunction, options, callback )
{
    if (!mongodb)
    {
        return callback('mongodb is not connected');
    }

    if ('function' == typeof options)
        callback = options;

    if ('function' != typeof prepareFunction)
        return callback('missing prepare function');
    var self = this;
    mongodb.collections(function(err,collections)
    {
        if (err)
        {
            callback(err);
        } else {
            self.migrateCollections(prepareFunction,options, collections,callback);
        }
    });

}


module.exports = Mongo2Influx;