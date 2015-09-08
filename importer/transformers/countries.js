var _ = require('lodash');
var async = require('async');

var name = 'countries';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT name, code FROM `countries` ORDER BY id';

    mysql.query(query, function __query(err, results) {
      if (err) { return done(err); }

      done(null, results);
    });
  },

  transform: function __transform(data, mysql, done) {
    done(null, _.map(data, function __map(country) {
      country.code = country.code.toUpperCase();

      return country;
    }));
  },

  dataOut: function __dataOut(data, mongodb, done) {
    async.waterfall([
      function(done) {
        mongodb.collection(name, function __collection(err, collection) {
          if (err) { return done(err); }
          done(null, collection);
        });
      },
      function(collection, done) {
        collection.remove({}, function __remove(err) {
          if (err) { return done(err); }
          done(null, collection);
        });
      },
      function(collection, done) {
        collection.insertMany(data, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
