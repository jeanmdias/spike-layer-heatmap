const fs = require('fs'); 
const csv = require('csv-parser');
const async = require('async');
const assert = require('assert');

const MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017',{useNewUrlParser: true}, function(err, client) {
   
    const db = client.db('spike-layer-heatmap');
    const collection = db.collection('airports');

    async.series([

        function(callback) {
            collection.deleteMany({}, function(err, result) {
                assert.equal(null,err);
                console.log('**************************************************************************************');
                console.log('** '+String(result.deletedCount)+' records deleted.');
                callback();
            });
        },

        function(callback) {
            fs.createReadStream('data/airports.csv')
            .pipe(csv())
            .on('data', function(data){
                try {
                    collection.insertOne(data, function(err, result) {
                        assert.equal(err, null);
                    });
                }
                catch(err) {
                    console.log('Error: ',err);
                }
            })
            .on('end',function(){
                console.log('** CSV loaded from http://ourairports.com/data/airports.csv');
                callback();
            });
        },
        
        function(callback) {
            var document = [];
            var listLegs = JSON.parse(fs.readFileSync('data/flightleg.json'));

            // TODO listLegs.locations.forEach(function(item) {
            async.forEach(listLegs.locations, function(item, callback) {
                
                collection.findOne({'iata_code':item.departureairportcode}, function(err, res) {
                    assert.equal(null, err);
                    if (!res) {
                        console.log('Aeroporto '+item.departureairportcode+' sem coordenadas');
                    } else {
                        document.push({
                            airportcode: item.departureairportcode,
                            latitude: 37.782551,
                            longitude: -122.445368
                        });
                    }
                });

                collection.findOne({'iata_code':item.arrivalairportcode}, function(err, res) {
                    assert.equal(null, err);
                    if (!res) {
                        console.log('Aeroporto '+item.arrivalairportcode+' sem coordenadas');
                    } else {
                        document.push({
                            airportcode: item.arrivalairportcode,
                            latitude: 37.782551,
                            longitude: -122.445368
                        });
                    }
                });
            
            }, function() {
                console.log('** Points list created');
                callback();
            });
            //});            
        },

        function(callback) {
            fs.writeFile('public/points.json', '{"locations":'+JSON.stringify(document)+'}');
            console.log('** JSON created successfully.');
            callback();
        },

    ], function() {
        client.close(function(err) {
          console.log('** MongoDB disconnected!');
          console.log('**************************************************************************************');
        });
    });

});