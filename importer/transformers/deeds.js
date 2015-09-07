var _ = require('lodash');

module.exports = {
  name: 'deeds',

  dataIn: function __dataIn(mysql, done) {
    console.log(this.name, 'dataIn');

    var query = 'SELECT d.title, d.image AS cover_image, d.video AS video_url, t.content ' +
      'FROM `deeds` AS d ' +
      'LEFT JOIN `translations` AS t ON t.deed_id = d.id AND t.language_id = 1 ' +
      'ORDER BY d.week';

    mysql.query(query, function __query(err, results) {
      if (err) { return done(err); }

      done(null, results);
    });
  },

  transform: function __transform(data, mysql, done) {
    console.log(this.name, 'transform');

    done(null, _.map(data, function __map(deed) {
      deed.url_title = _.kebabCase(deed.title);
      deed.created = new Date();

      deed.video_url = _.last(/src="([^"]*)"/.exec(deed.video_url));

      return deed;
    }));
  },

  dataOut: function __dataOut(data, mongodb, done) {
    console.log(this.name, 'dataOut');

    mongodb.collection(this.name, function __collection(err, collection) {
      if (err) { return done(err); }

      collection.remove({}, function __remove(err) {
        if (err) { return done(err); }

        collection.insertMany(data, null, function __insertMany(err, result) {
          if (err) { return done(err); }

          done(null, result);
        });
      });
    });
  }
};
