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

    processTransformations(mysql, mongodb)
  });
});

function processTransformations(mysql, mongodb) {
  // Get all transformers
  var transformers = _.map(config.transformers, function __map(transformer) {
    return require('./transformers/' + transformer);
  });

  // Run each transformer
  _.each(transformers, function __each(transformer) {
    var data = transformer.dataIn(mysql);
    if (_.isFunction(transformer.transform)) {
      data = transformer.transform(data, mysql);
    }
    transformer.dataOut(data, mongodb);
  });

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
}
