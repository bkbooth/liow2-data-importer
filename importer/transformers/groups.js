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
              group.owner = users[0]._id;
              group.admins = [group.owner]; // TODO: get all admins?
              group.country = group.country ? _.find(countries, 'code', group.country.toUpperCase())._id : null;

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
    ], done);
  }
};
