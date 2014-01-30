

var Mongo2Influx = require('../index.js');



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

        }
    });


function prepareData(row)
{
    // remove the mongoDB id
    delete(row['_id']);

    //Assuming your mongodb rows contain a mongoDB date, copy date to time
    row.time = row.date;

    // delete unwanted fields
    delete(row.date);
    return row;
}


mongo2influx.connect(function(err)
{
    if (err)
    {
        throw err;
    } else {
        console.log('connected');
        mongo2influx.migrate(prepareData,function(err)
        {
            if (err)
            {
                throw err;
            } else {
                console.log('done migrating');
            }
        });
    }
});