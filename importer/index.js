var _ = require('lodash');
var Promise = require('bluebird');
var config = require('./config');

var mysql = require('mysql').createConnection(config.mysql);
mysql.connect(function __connect(err) {
  if (err) { console.error('Failed connecting to MySQL: ' + err.message); return false; }

  console.log('Connected to MySQL');

  // Connect to MongoDB
  require('mongodb').MongoClient.connect(config.mongo, function(err, mongodb) {
    if (err) {
      console.error('Failed connecting to MongoDB: ' + err.message);
      mysql.destroy();
      return false;
    }

    console.log('Connected to MongoDB');

    processTransformations(mysql, mongodb, function __done(err) {
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
  });
});

function processTransformations(mysql, mongodb, done) {
  // Get all transformers
  var transformers = _.map(config.transformers, function __map(transformer) {
    return require('./transformers/' + transformer);
  });

  // Run each transformer
  _.each(transformers, function __each(transformer, index) {
    transformer.dataIn(mysql, function __dataIn(err, data) {
      if (err) { return done(err); }

      transformer.transform(data, mysql, function __transform(err, data) {
        if (err) { return done(err); }

        transformer.dataOut(data, mongodb, function __dataOut(err, result) {
          if (err) { return done(err); }

          console.log('Finished importing', result.insertedCount, transformer.name);

          if (index + 1 === transformers.length) { done(); }
        });
      });
    });
  });
}
