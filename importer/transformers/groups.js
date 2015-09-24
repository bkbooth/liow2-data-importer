var _ = require('lodash');
var async = require('async');

var name = 'groups';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT DISTINCT m.name, m.letter AS welcome_message, m.image AS logo, m.created, c.code as country ' +
      'FROM `ministries` AS m ' +
      'RIGHT JOIN `users` AS u ON u.ministry_id = m.id ' +
      'RIGHT JOIN `testimonies` AS t ON t.user_id = u.id ' +
      'LEFT JOIN `countries` AS c ON m.country_id = c.id ' +
      'WHERE m.name IS NOT NULL AND u.activated = 1 ' +
      'ORDER BY m.id';

    mysql.query(query, function __query(err, groups) {
      if (err) { return done(err); }

      done(null, groups);
    });
  },

  transform: function __transform(groups, mysql, mongodb, done) {
    async.waterfall([
      function(done) {
        mongodb.collection('countries').find({}, { _id: true, code: true }).toArray(done);
      },
      function(countries, done) {
        async.map(groups, function(group, done) {
          mongodb.collection('users')
            .find({ groups: group.name }, { _id: true, email: true, admin: true })
            .sort({ _id: 1 })
            .toArray(function __toArray(err, users) {
              if (err) { return done(err); }

              group.url_name = _.kebabCase(group.name);
              group.country = group.country ? _.find(countries, 'code', group.country.toUpperCase())._id : null;

              group.admins = _.pluck(_.filter(users, function(user) { return user.admin < 4; }), '_id');
              group.owner = group.admins[0];

              done(null, group);
            });
        }, done);
      }
    ], done);
  },

  dataOut: function __dataOut(groups, mongodb, done) {
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
        collection.insertMany(groups, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], function(err, result) {
      if (err) { return done(err); }

      // After saving groups, need to update each users' group
      mongodb.collection('users').find().toArray(function __toArray(err, users) {
        if (err) { return done(err); }

        async.each(users, function __each(user, done) {
          var group = user.groups && user.groups[0] ? _.find(groups, 'name', user.groups[0]) : null;
          user.groups = group ? [group._id] : [];

          done(null, mongodb.collection('users').save(user));
        }, function(err) {
          if (err) { return done(err); }

          console.log('Finished updating', users.length, 'user groups');
          done(null, result);
        });
      });
    });
  }
};
