

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
    logging : true

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

}

Mongo2Influx.prototype.connect = function (cb)
{

    influxServer = influx(
        configuration.influxserver.hostname,
        configuration.influxserver.port,
        configuration.influxserver.user,
        configuration.influxserver.pass,
        configuration.influxserver.database
    );

    influxDB = influx(
        configuration.influxdb.hostname,
        configuration.influxdb.port,
        configuration.influxdb.user,
        configuration.influxdb.pass,
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
        self.log('found',collections.length,'collection');
        async.eachSeries(collections,function(collection,callbackCollections)
        {

            var collectionName = collection.collectionName;
            if ( -1 !== collectionName.indexOf('system')) return callbackCollections();
            self.log('next collection: ',collectionName);
            var startDump = new Date();
            collection.find().toArray(function(err, results) {
                if (!err && _.isArray(results))
                {
                    self.log('reading results from',collectionName,results.length,'rows, took',(new Date()-startDump),'ms');

                    var index =0;
                    async.eachLimit(results,8,function(row,cb){
                        var startMigration = new Date();
                        var data = prepareFunction(row);

                        if (!row.time) {
                            self.log('skipped row',row);
                            return cb();
                        }

                        influxDB.writePoint(collectionName, data , {pool : false}, function(err) {
                            index++;
                            if (0 == index%1000)
                            {
                                var diff = (new Date()-startMigration) / 1000;
                                var ips = 1000 / diff;
                                self.log('collection',collectionName,'item #',index,'@',ips,' inserts/sec');
                                startMigration = new Date();
                            }
                            if (cb) cb();
                        });
                    },function(err,res)
                    {
                        self.log('collection done',collectionName);
                        callbackCollections(err);
                    });
                } else {
                    self.log('error',err);
                    callbackCollections(err);
                }
            });

        },function(err)
        {
            self.log('done with everything');
        });
    });

}


module.exports = Mongo2Influx;