const fs = require('fs'); 
const csv = require('csv-parser');

const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

MongoClient.connect('mongodb://localhost:27017',{useNewUrlParser: true}, function(err, client) {
   
    const db = client.db('spike-layer-heatmap');
    const collection = db.collection('airports');

    collection.deleteMany({}, function(err, result) {
        assert.equal(null,err);
        console.log(String(result.deletedCount)+" records deleted.");
    });

    fs.createReadStream('data/airports.csv')
    .pipe(csv())
    .on('data', function(data){
        try {
            //if(data['scheduled_service'] == 'yes') {
                collection.insertOne(data, function(err, result) {
                    assert.equal(err, null);
                });
            //}
        }
        catch(err) {
            console.log('Error: ',err);
        }
    })
    .on('end',function(){
        console.log('MongoClient disconnected.');
        client.close();
    });
});