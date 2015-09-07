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
  mongo: 'mongodb://localhost:27017/database',

  // Names of modules inside ./transformers directory
  // Must export dataIn, transform and dataOut functions
  transformers: []

};
