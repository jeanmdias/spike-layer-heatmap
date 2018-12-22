const fs = require('fs'); 
const csv = require('csv-parser');
const async = require('async');
const assert = require('assert');

require('dotenv').config();
var url = process.env.MONGODB_URI;

const MongoClient = require('mongodb').MongoClient;

console.log('**************************************************************************************');

MongoClient.connect(url,{useNewUrlParser: true}, function(err, client) {
   
    const db = client.db('spike-flights-heatmap');
    const col_airports = db.collection('airport');
    const col_flightlg = db.collection('flights');

    var listLegs;

    async.series([

        /*
        function(seriesCallback) {
            col_airports.createIndex( { 'iata_code': 1 }, function(err, result) {
                assert.equal(null,err);
                console.log('** Index created');
                seriesCallback();
            });
        },

        function(seriesCallback) {
            col_flightlg.createIndex( { 'airportcode': 1 }, function(err, result) {
                assert.equal(null,err);
                console.log('** Index created');
                seriesCallback();
            });
        },
        */

        function(seriesCallback) {
            col_airports.deleteMany({}, function(err, result) {
                assert.equal(null,err);
                console.log('** '+String(result.deletedCount)+' records deleted at collection "airport"');
                seriesCallback();
            });
        },

        function(seriesCallback) {
            col_flightlg.deleteMany({}, function(err, result) {
                assert.equal(null,err);
                console.log('** '+String(result.deletedCount)+' records deleted at collection "flights"');
                seriesCallback();
            });
        },

        function(seriesCallback) {
            fs.createReadStream('data/airports.csv')
            .pipe(csv())
            .on('data', function(data){
                try {
                    col_airports.insertOne(data, function(err, result) {
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
                        col_airports.findOne({'iata_code':item.departureairportcode}, function(err, res) {
                            assert.equal(null, err);
                            if (!res) {
                                console.log('Aeroporto '+item.departureairportcode+' sem coordenadas');
                                callback();
                            } else {
                                var document = {
                                    airportcode: item.departureairportcode,
                                    lat: res.latitude_deg,
                                    lng: res.longitude_deg
                                }
                                col_flightlg.insertOne(document, function(err, result) {
                                    assert.equal(err, null);
                                    callback();
                                });
                            }
                        });
                    },

                    // Arrival
                    function(callback) {
                        col_airports.findOne({'iata_code':item.arrivalairportcode}, function(err, res) {
                            assert.equal(null, err);
                            if (!res) {
                                console.log('Aeroporto '+item.arrivalairportcode+' sem coordenadas');
                                callback();
                            } else {
                                var document = {
                                    airportcode: item.arrivalairportcode,
                                    lat: res.latitude_deg,
                                    lng: res.longitude_deg
                                }
                                col_flightlg.insertOne(document, function(err, result) {
                                    assert.equal(err, null);
                                    callback();
                                });
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

            var export_points = '{"locations":[';
            var format_points = false;

            col_airports.aggregate([
                {
                    $lookup: 
                    {   
                        from: "flights", 
                        localField: "iata_code", 
                        foreignField: "airportcode", 
                        as: "legs"
                    }
                },
                {
                    $project:
                    {
                        _id:0,
                        iata_code: "$iata_code",
                        latitude_deg: "$latitude_deg",
                        longitude_deg: "$longitude_deg",
                        numOfPoints: { $size: "$legs" }
                    }
                },
                { 
                    $match: 
                    { 
                        numOfPoints: { $gt: 0 } 
                    } 
                }
            ]).toArray(function(err, points) {
                
                //console.log(points);
                async.forEach(points, function(point, eachCallback) {

                    async.series([

                        function(callback) {
                            if(format_points)
                            {
                                export_points = export_points + ',';
                            }
                            export_points = export_points + '{"lat":"'+point.latitude_deg+'","lng":"'+point.longitude_deg+'", "weight":"'+point.numOfPoints+'"}';
                            
                            format_points = true;
                            callback();
                        }

                    ], function(err, results) {
                        eachCallback();
                    }); 
                            

                }, function() {
                    fs.writeFile('public/points.json', export_points+']}');
                    console.log('** Points list exported');
                    seriesCallback();
                });   

            });

        }

    ], function() {

        client.close(function(err) {
          console.log('** MongoDB disconnected!');
          console.log('**************************************************************************************');
        });
        
    });
});