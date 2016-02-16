var config = require('../config');
var _ = require('lodash');
var async = require('async');
var toMarkdown = require('to-markdown');

module.exports = {
  name: 'testimonies',

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT u.email AS user, d.title AS deed, t.content, t.created, t.modified ' +
      'FROM `testimonies` AS t ' +
      'LEFT JOIN `users` AS u ON u.id = t.user_id ' +
      'LEFT JOIN `deeds` AS d ON d.id = t.deed_id ' +
      'WHERE t.content IS NOT NULL ' +
      'ORDER BY t.id';

    mysql.query(query, function __query(err, testimonies) {
      if (err) { return done(err); }

      done(null, testimonies);
    });
  },

  transform: function __transform(testimonies, mysql, mongodb, done) {
    async.parallel([
      function(done) {
        mongodb.collection('users').find({}, { _id: true, email: true }).toArray(done);
      },
      function(done) {
        mongodb.collection('deeds').find({}, { _id: true, title: true }).toArray(done);
      }
    ], function(err, results) {
      if (err) { return done(err); }

      var users = results[0];
      var deeds = results[1];

      done(null, _.filter(_.map(testimonies, function __map(testimony) {
        var user = _.find(users, ['email', testimony.user]);
        testimony.user = user ? user._id : null;

        var deed = _.find(deeds, ['title', testimony.deed]);
        testimony.target = { deed: deed ? deed._id : null };
        delete testimony.deed;

        testimony.content = {
          text: toMarkdown(testimony.content, { converters: config.markdownConverters })
        };

        if (testimony.created.valueOf() === testimony.modified.valueOf()) {
          delete testimony.modified;
        }

        return testimony;
      }), function __filter(testimony) {
        return testimony.user !== null && testimony.target.deed !== null;
      }));
    });
  },

  dataOut: function __dataOut(testimonies, mongodb, done) {
    async.waterfall([
      function(done) {
        mongodb.collection('comments', function __collection(err, collection) {
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
        collection.insertMany(testimonies, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
