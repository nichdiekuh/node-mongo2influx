

var _ = require('underscore');
var async = require('async');
var influx  = require('influx');

var charm = require('charm')();

charm.pipe(process.stdout);
charm.on('^C', function () {
   charm.down(2).column(0).foreground('white');
   process.exit();
});

var logBuffer = [];

var MongoClient = require('mongodb').MongoClient
    , Server = require('mongodb').Server;

var ObjectID = require('mongodb').ObjectID;

var collectionsInProgres  = {};
var collectionsIndex = 0;
var collectionsCount = 0;

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
        database    : 'default',
        querylimit  : 100000
    },
    logging         : true,
    limit           : 2,
    insertlimit     : 100,
    emptySeries     : false

};

var influxServer, influxDB, mongoClient, mongodb;

var drawInterval = false;


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
    if (options.emptySeries) configuration.emptySeries = options.emptySeries;
    if (options.insertlimit) configuration.insertlimit = options.insertlimit;

};

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
        return cb(null);

    });
};

Mongo2Influx.prototype.draw = function()
{
    charm.reset();

    if (!configuration.logging) return;
    charm.foreground('white');
    charm.write('--=[log]=---------------------------\n');
     _.each(logBuffer,function(line)
    {
        charm.column(0);
        charm.write(line);
        charm.down();
    });
    charm.down(18-logBuffer.length);
    charm.column(0);
    var progress = 100 / collectionsCount * collectionsIndex;
    charm.write('Overall progress ').foreground('green').write(Math.round(progress)+'%');
    charm.foreground('white').write('('+collectionsIndex+'/'+collectionsCount+')');
    charm.down();
    charm.down();
    charm.column(0);
    charm.write('--=[current collections]=---------------------------\n');
    _.each(collectionsInProgres,function(col,name)
    {
        charm.column(0);
        charm.write(name);
        charm.column(30);
        charm.write(col.state);

        charm.column(45);
        charm.foreground('green');
        charm.write(Math.round(col.progress)+'%');
        charm.foreground('white');
        charm.column(50);
        var ips = Math.round(col.ips).toString();
        if (0 != ips)
        {
            charm.foreground('blue').write(ips).write(' inserts/sec');
            charm.foreground('white');
        }
        charm.down();
    });
    charm.column(0);

};



Mongo2Influx.prototype.log = function ()
{
    if (configuration.logging) {
        var date = new Date();
        var line = date.getHours()+':'+date.getMinutes()+':'+date.getSeconds();
        line += ': '+_.values(arguments).join(' ');
        logBuffer.push(line);
        while (15 < logBuffer.length) logBuffer.shift();
    }
};

Mongo2Influx.prototype.updateCollection = function(collectionName,values)
{
    _.extend(collectionsInProgres[collectionName],values);
};



Mongo2Influx.prototype.migrateCollection = function(prepareFunction, collection,callbackCollections)
{
    var self = this;
    var collectionName = collection.collectionName;
    var startDump = new Date();

    self.countItems(collection,function(err,itemCount)
    {
        if (err) return callbackCollections(err);
        var jobCount = Math.ceil(itemCount/configuration.mongodb.querylimit);
        var mongoJobs = [];
        for (var i=0; i<jobCount;++i)
            mongoJobs.push(i*configuration.mongodb.querylimit);

        var rowsSkipped = 0;

        var lastID

        async.eachSeries(mongoJobs,function(mongoOffset,callbackFind)
        {

            self.updateCollection(collectionName,{state : 'reading'});

            var where;
            if (lastID)
            {
                where = {'_id' : {'$gt' : lastID }};
            }

            var cursor = collection.find(where).sort({_id : 1}).limit(configuration.mongodb.querylimit);

            cursor.toArray(function(err, results) {
                if (!err && _.isArray(results))
                {
                    self.log('reading results from',collectionName,results.length,'rows, took',(new Date()-startDump),'ms');
                    self.updateCollection(collectionName,{state : 'inserting'});


                    var index =0;
                    var lastIndex =0;

                    var startMigration = new Date();

                    var jobCount = Math.ceil(results.length/configuration.insertlimit);
                    var jobs = [];
                    for (var i=0; i<jobCount;++i)
                        jobs.push(i*configuration.insertlimit);

                    lastID = results[results.length-1]['_id'];

                    var bench = function()
                    {
                        var inserts = index-lastIndex;
                        lastIndex=index;
                        var diff = (new Date()-startMigration) / 1000;
                        var ips = Math.round(inserts/ diff);
                        startMigration = new Date();
                        var progress = 100 / itemCount * (index+mongoOffset);
                        self.updateCollection(collectionName,{state : 'inserting',progress : progress,ips: ips,item:index});
                    };

//                    var statInterval = setInterval(bench,500);

                    async.eachSeries(jobs,function(offset,cb){
                        var data = [];
                        var offsetLimit = offset + configuration.insertlimit -1;
                        if (offsetLimit >= results.length) offsetLimit = results.length-1;

                        for (var i=offset;i<=offsetLimit;++i)
                        {
                            var row = prepareFunction(results[i]);
                            if (!row.time) {
                                rowsSkipped++;
                            } else {
                                data.push(row);
                            }
                        }

                        influxDB.writePoints(collectionName, data , {pool : false}, function(err) {
                            bench();
                            if (err)
                            {
                                return cb(err)
                            }
                            else {
                                index += data.length;
                                delete(data);
                                return cb();
                            }
                        });
                    },function(err)
                    {
//                        clearInterval(statInterval);
                        return callbackFind(err);
                    });
                } else {
                    results = null;
                    delete(results);
                    return callbackFind(err);
                }
            });
        },function(err)
        {
            if (err)
            {
                self.log('error migrating collection',collectionName);

            } else {
                var successRate = 100 / itemCount * (itemCount-rowsSkipped);
                self.log('collection',collectionName,'done, skipped',rowsSkipped,'rows, successrate:',successRate,'%');
            }
            delete(collectionsInProgres[collectionName]);
            callbackCollections(err);
        });
    });
};


Mongo2Influx.prototype.countItems = function (collection,callback)
{
    collection.count(function(err,count)
    {
        callback(err,count);
    });
};


Mongo2Influx.prototype.migrateCollections = function( prepareFunction, options, collections, callback )
{
    var self = this;
    self.log('found',collections.length,'collection');
    async.eachLimit(collections,configuration.limit,function( collection, callbackCollections )
    {
        collectionsIndex++;
        var collectionName = collection.collectionName;
        if ( -1 !== collectionName.indexOf('system')) return callbackCollections();
        self.log('next collection: ',collectionName);

        collectionsInProgres[collectionName] = {
            state       : '',
            progress    : 0,
            ips         : 0,
            item        : 0
        };
        if (true === configuration.emptySeries)
        {
            self.emptySeries(collection.collectionName,function()
            {
                self.migrateCollection(prepareFunction, collection,callbackCollections);
            })
        } else {
            self.migrateCollection(prepareFunction, collection,callbackCollections);
        }
    },callback);
};



Mongo2Influx.prototype.emptySeries = function(collectionName,callback)
{
    var self = this;
    var start = new Date();
    self.log('Truncating influx series',collectionName);
    self.updateCollection(collectionName,{state : 'truncating'});

    influxDB.query('DELETE FROM '+collectionName+' WHERE time < now();',function(err)
    {
        var diff = new Date()-start;
        self.updateCollection(collectionName,{state : 'truncated'});

        self.log('Truncating influx series',collectionName,'took',diff,'ms');
        callback(err);
    });
};



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
    drawInterval = setInterval(self.draw,500);
    collectionsIndex = 0;
    mongodb.collections(function(err,collections)
    {
        if (err)
        {
            callback(err);
        } else {
            collectionsCount = collections.length;
            self.migrateCollections(prepareFunction,options, collections,function(err)
            {
                clearInterval(drawInterval);
                callback(err);
            });
        }
    });
};


module.exports = Mongo2Influx;