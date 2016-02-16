module.exports = {

  // Pass straight into mysql.createConnection()
  mysql: {
    host: 'localhost',
    port: 3306,
    user: 'user',
    password: 'password',
    database: 'database'
  },

  // Pass straight into MongoClient.connect()
  mongodb: 'mongodb://localhost:27017/database',

  // Names of modules inside ./transformers directory
  // Must export dataIn, transform and dataOut functions
  transformers: [
    'countries',
    'deeds',
    'users',
    'groups',
    'testimonies',
    'acts',
    'news'
  ],

  // Extra converters to pass to toMarkdown
  markdownConverters: [{
    // Remove spans and divs with styling
    filter: function (node) {
      return (node.nodeName === 'SPAN' || node.nodeName === 'DIV') && node.style;
    },
    replacement: function (content) {
      return content;
    }
  }, {
    // Remove addthis sharing divs
    filter: function (node) {
      return node.nodeName === 'DIV' && /addthis/.test(node.className);
    },
    replacement: function () {
      return '';
    }
  }]

};
