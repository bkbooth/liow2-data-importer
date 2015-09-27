var config = require('../config');
var _ = require('lodash');
var async = require('async');
var toMarkdown = require('to-markdown');

module.exports = {
  name: 'news',

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT n.title, n.content, n.created, n.modified, m.name AS \'group\', c.code AS country ' +
      'FROM `news` AS n ' +
      'LEFT JOIN `ministries` AS m ON n.ministry_id = m.id ' +
      'LEFT JOIN `countries` AS c ON n.country_id = c.id ' +
      'ORDER BY n.id';

    mysql.query(query, function __query(err, news) {
      if (err) { return done(err); }

      done(null, news);
    });
  },

  transform: function __transform(news, mysql, mongodb, done) {
    async.parallel([
      function(done) {
        mongodb.collection('groups').find({}, { _id: true, name: true, owner: true }).toArray(done);
      },
      function(done) {
        mongodb.collection('countries').find({}, { _id: true, code: true }).toArray(done);
      }
    ], function(err, results) {
      if (err) { return done(err); }

      var groups = results[0];
      var countries = results[1];

      done(null, _.filter(_.map(news, function __map(item) {
        var group = item.group ? _.find(groups, 'name', item.group) : null;
        var country = item.country ? _.find(countries, 'code', item.country.toUpperCase()) : null;

        if (group) {
          item.target = { group: group._id };
        } else if (country) {
          item.target = { country: country._id };
        }

        delete item.group;
        delete item.country;

        item.url_title = _.kebabCase(item.title.toLowerCase());
        item.author = group ? group.owner : null;
        item.content = {
          text: toMarkdown(item.content, { converters: config.markdownConverters })
        };

        if (item.created.valueOf() === item.modified.valueOf()) {
          delete item.modified;
        }

        return item;
      }), function __filter(item) {
        return item.target;
      }));
    });
  },

  dataOut: function __dataOut(deeds, mongodb, done) {
    async.waterfall([
      function(done) {
        mongodb.collection('comments', function __collection(err, collection) {
          if (err) { return done(err); }
          done(null, collection);
        });
      },
      // Don't clear comments, testimonies have been inserted
      function(collection, done) {
        collection.insertMany(deeds, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
