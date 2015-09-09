var _ = require('lodash');
var async = require('async');

var name = 'users';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT u.email, u.name, u.group_id as admin, u.created, ' +
      'IF(u.activated = 1, TRUE, FALSE) AS activated, c.code as country ' +
      'FROM `users` AS u ' +
      'LEFT JOIN `countries` AS c ON u.country_id = c.id ' +
      'ORDER BY u.id';

    mysql.query(query, function __query(err, users) {
      if (err) { return done(err); }

      done(null, users);
    });
  },

  transform: function __transform(users, mysql, mongodb, done) {
    async.parallel([
      function(done) {
        mongodb.collection('countries').find({}).toArray(done);
      },
      function(done) {
        async.filter(users, function __filter(user, done) {
          done(user.activated);
        }, _.partial(done, null));
      }
    ], function __done(err, results) {
      var countries = results[0];
      var users = results[1];

      async.map(users, function(user, done) {
        user.username = user.name;
        user.admin = user.admin === 1;
        user.activated = Boolean(user.activated);
        user.country = _.find(countries, 'code', user.country.toUpperCase())._id;

        done(null, user);
      }, done);
    });
  },

  dataOut: function __dataOut(users, mongodb, done) {
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
        collection.insertMany(users, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
