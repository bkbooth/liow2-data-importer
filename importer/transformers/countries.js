var _ = require('lodash');
var async = require('async');

var name = 'countries';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT name, code FROM `countries` ORDER BY id';

    mysql.query(query, function __query(err, countries) {
      if (err) { return done(err); }

      done(null, countries);
    });
  },

  transform: function __transform(countries, mysql, mongodb, done) {
    done(null, _.map(countries, function __map(country) {
      country.code = country.code.toUpperCase();

      return country;
    }));
  },

  dataOut: function __dataOut(countries, mongodb, done) {
    async.waterfall([
      function(done) {
        mongodb.collection(name, function __collection(err, collection) {
          if (err) { return done(err); }
          done(null, collection);
        });
      },
      function(collection, done) {
        collection.deleteMany({}, function __deleteMany(err) {
          if (err) { return done(err); }
          done(null, collection);
        });
      },
      function(collection, done) {
        collection.insertMany(countries, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
