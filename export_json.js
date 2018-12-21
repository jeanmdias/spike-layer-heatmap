const fs = require('fs'); 
const csv = require('csv-parser');
const async = require('async');
const assert = require('assert');

const MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017',{useNewUrlParser: true}, function(err, client) {
   
    const db = client.db('spike-layer-heatmap');
    const collection = db.collection('airports');

    var document = [];
    var listLegs;

    async.series([

        function(seriesCallback) {
            collection.deleteMany({}, function(err, result) {
                assert.equal(null,err);
                console.log('**************************************************************************************');
                console.log('** '+String(result.deletedCount)+' records deleted.');
                seriesCallback();
            });
        },

        function(seriesCallback) {
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
                console.log('** List of airports loaded from http://ourairports.com/data/airports.csv');
                seriesCallback();
            });
        },
        
        function(seriesCallback) {
            listLegs = JSON.parse(fs.readFileSync('data/flightleg.json'));
            console.log('** List of flightlegs loaded');
            seriesCallback();
        },
        
        function(seriesCallback) {

            async.forEach(listLegs.locations, function(item, eachCallback) {
                
                async.series([

                    // Departure
                    function(callback) {
                        collection.findOne({'iata_code':item.departureairportcode}, function(err, res) {
                            assert.equal(null, err);
                            if (!res) {
                                console.log('Aeroporto '+item.departureairportcode+' sem coordenadas');
                                callback();
                            } else {
                                document.push({
                                    lat: res.latitude_deg,
                                    lng: res.longitude_deg
                                });
                                callback();
                            }
                        });
                    },

                    // Arrival
                    function(callback) {
                        collection.findOne({'iata_code':item.arrivalairportcode}, function(err, res) {
                            assert.equal(null, err);
                            if (!res) {
                                console.log('Aeroporto '+item.arrivalairportcode+' sem coordenadas');
                                callback();
                            } else {
                                document.push({
                                    lat: res.latitude_deg,
                                    lng: res.longitude_deg
                                });
                                callback();
                            }
                        }); 
                    }

                ], function(err, results) {
                    eachCallback();
                }); 

            }, function() {
                console.log('** Points list created');
                seriesCallback();
            });         
        },
        
        function(seriesCallback) {
            fs.writeFile('public/points.json', '{"locations":'+JSON.stringify(document)+'}');
            console.log('** JSON created successfully.');
            seriesCallback();
        },

    ], function() {
        client.close(function(err) {
          console.log('** MongoDB disconnected!');
          console.log('**************************************************************************************');
        });
    });

});