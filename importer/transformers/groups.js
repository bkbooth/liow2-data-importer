var config = require('../config');
var _ = require('lodash');
var async = require('async');
var toMarkdown = require('to-markdown');

var name = 'groups';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT m.name, m.letter AS welcomeMessage, m.image AS logo, m.created, c.code as country ' +
      'FROM `ministries` AS m ' +
      'INNER JOIN `users` AS u ON u.ministry_id = m.id ' +
      'INNER JOIN `testimonies` AS t ON t.user_id = u.id ' +
      'LEFT JOIN `countries` AS c ON m.country_id = c.id ' +
      'WHERE m.name IS NOT NULL AND u.activated = 1 ' +
      'GROUP BY m.id ' +
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
        groups = _.filter(groups, function __filter(group) { return group.name !== 'Global Campaign' });

        async.map(groups, function(group, done) {
          mongodb.collection('users')
            .find({ groups: group.name }, { _id: true, email: true, admin: true })
            .sort({ _id: 1 })
            .toArray(function __toArray(err, users) {
              if (err) { return done(err); }

              group.urlName = _.kebabCase(group.name);
              if (group.country) group.country = _.find(countries, ['code', group.country.toUpperCase()])._id;
              group.welcomeMessage = toMarkdown(group.welcomeMessage, { converters: config.markdownConverters });

              group.admins = _.map(_.filter(users, function(user) { return user.admin < 4; }), '_id');
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

      async.parallel([
        function(done) {
          // Update each users' group
          mongodb.collection('users').find().toArray(function __toArray(err, users) {
            if (err) { return done(err); }

            async.each(users, function __each(user, done) {
              var group = user.groups && user.groups[0] ? _.find(groups, ['name', user.groups[0]]) : null;
              user.groups = group ? [group._id] : [];

              delete user.admin;

              done(null, mongodb.collection('users').save(user));
            }, function(err) {
              if (err) { return done(err); }

              console.log('Finished updating', users.length, 'user groups');
              done(null, result);
            });
          });
        },
        function(done) {
          // Create an 11-week campaign for each group
          mongodb.collection('deeds').find().toArray(function __toArray(err, deeds) {
            var campaigns = _.map(groups, function (group) {
              return {
                group: group._id,
                dateStart: group.created,
                dateEnd: new Date(),
                active: false,
                deeds: _.map(deeds, function(deed) { return { deed: deed._id, published: true }; }),
                created: group.created
              };
            });

            async.waterfall([
              function(done) {
                mongodb.collection('campaigns', function __collection(err, collection) {
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
                collection.insertMany(campaigns, null, function __insertMany(err, result) {
                  if (err) { return done(err); }
                  done(null, result);
                });
              }
            ], function(err) {
              if (err) { return done(err); }

              console.log('Finished adding', campaigns.length, 'campaigns');
              done(null, result);
            });
          });
        }
      ], done(null, result));
    });
  }
};
