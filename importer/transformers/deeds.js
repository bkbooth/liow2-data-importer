var _ = require('lodash');
var async = require('async');

var name = 'deeds';

module.exports = {
  name: name,

  dataIn: function __dataIn(mysql, done) {
    var query = 'SELECT d.title, d.image AS cover_image, d.video AS video_url, t.content ' +
      'FROM `deeds` AS d ' +
      'LEFT JOIN `translations` AS t ON t.deed_id = d.id AND t.language_id = 1 ' +
      'ORDER BY d.week';

    mysql.query(query, function __query(err, deeds) {
      if (err) { return done(err); }

      done(null, deeds);
    });
  },

  transform: function __transform(deeds, mysql, mongodb, done) {
    done(null, _.map(deeds, function __map(deed) {
      deed.url_title = _.kebabCase(deed.title);
      deed.video_url = _.last(/src="([^"]*)"/.exec(deed.video_url));
      deed.created = new Date();

      return deed;
    }));
  },

  dataOut: function __dataOut(deeds, mongodb, done) {
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
        collection.insertMany(deeds, null, function __insertMany(err, result) {
          if (err) { return done(err); }
          done(null, result);
        });
      }
    ], done);
  }
};
