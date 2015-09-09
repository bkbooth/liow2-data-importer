var _ = require('lodash');
var async = require('async');
var config = require('./config');

async.waterfall([
  function(done) {
    // Connect to MySQL
    var mysql = require('mysql').createConnection(config.mysql);
    mysql.connect(function __connect(err) {
      if (err) { return done(err); }

      console.log('Connected to MySQL');
      done(null, mysql);
    });
  },
  function(mysql, done) {
    // Connect to MongoDB
    require('mongodb').MongoClient.connect(config.mongodb, function(err, mongodb) {
      if (err) { return done(err); }

      console.log('Connected to MongoDB');
      done(null, mysql, mongodb);
    });
  },
  function(mysql, mongodb, done) {
    processTransformations(mysql, mongodb, function __done(err) {
      if (err) { done(err); }

      done(null, mysql, mongodb);
    });
  }
], function __done(err, mysql, mongodb) {
  if (err) { console.log('Error: ' + err.message); }

  // Disconnect from MySQL
  mysql.end(function __end(err) {
    if (err) { throw new Error('Failed disconnecting from MySQL: ' + err.message) }
    console.log('Disconnected from MySQL');
  });

  // Disconnect from MongoDB
  mongodb.close(function __close(err) {
    if (err) { throw new Error('Failed disconnecting from MongoDB: ' + err.message) }
    console.log('Disconnected from MongoDB');
  });
});

function processTransformations(mysql, mongodb, done) {
  // Get all transformers
  var transformers = _.map(config.transformers, function __map(transformer) {
    return require('./transformers/' + transformer);
  });

  // Run each transformer in series
  async.eachSeries(transformers, function __eachSeries(transformer, done) {
    async.waterfall([
      function(done) {
        transformer.dataIn(mysql, done);
      },
      function(data, done) {
        transformer.transform(data, mysql, mongodb, done);
      },
      function(data, done) {
        transformer.dataOut(data, mongodb, done);
      }
    ], function __done(err, result) {
      if (err) { return done(err); }

      console.log('Finished importing', result.insertedCount, transformer.name);
      done();
    });
  }, done);
}
