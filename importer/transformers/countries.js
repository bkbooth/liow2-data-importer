var _ = require('lodash');

module.exports = {
  name: 'countries',

  dataIn: function __dataIn(mysql, done) {
    console.log(this.name, 'dataIn');

    var query = 'SELECT name, code FROM `countries` ORDER BY id';
    mysql.query(query, function __query(err, results) {
      if (err) { return done(err); }

      done(null, results);
    });
  },

  transform: function __transform(data, mysql, done) {
    console.log(this.name, 'transform');

    done(null, _.map(data, function __map(country) {
      country.code = country.code.toUpperCase();

      return country;
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
