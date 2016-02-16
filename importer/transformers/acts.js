var _ = require('lodash');
var async = require('async');

var name = 'acts';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT u.email AS user, d.title AS deed, t.created ' +
      'FROM `testimonies` AS t ' +
      'LEFT JOIN `users` AS u ON u.id = t.user_id ' +
      'LEFT JOIN `deeds` AS d ON d.id = t.deed_id ' +
      'ORDER BY t.id';

    mysql.query(query, function __query(err, acts) {
      if (err) { return done(err); }

      done(null, acts);
    });
  },

  transform: function __transform(acts, mysql, mongodb, done) {
    async.parallel([
      function(done) {
        mongodb.collection('users').find({}, { _id: true, email: true, groups: true }).toArray(done);
      },
      function(done) {
        mongodb.collection('deeds').find({}, { _id: true, title: true }).toArray(done);
      },
      function(done) {
        mongodb.collection('campaigns').find({}, { _id: true, group: true }).toArray(done);
      }
    ], function(err, results) {
      if (err) { return done(err); }

      var users = results[0];
      var deeds = results[1];
      var campaigns = results[2];

      done(null, _.filter(_.map(acts, function __map(act) {
        var user = _.find(users, ['email', act.user]);
        var deed = _.find(deeds, ['title', act.deed]);

        act.user = user ? user._id : null;
        act.deed = deed ? deed._id : null;
        if (user && user.groups[0]) {
          act.group = user.groups[0];
          act.campaign = _.find(campaigns, ['group', act.group])._id;
        }

        return act;
      }), function __filter(act) {
        return act.user !== null && act.deed !== null;
      }));
    });
  },

  dataOut: function __dataOut(acts, mongodb, done) {
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
        collection.insertMany(acts, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
