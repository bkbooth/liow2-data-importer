module.exports = {
  dataIn: function __dataIn(mysql) {
    console.log('dataIn');
    return [];
  },

  transform: function __transform(data, mysql) {
    console.log('transform', data);
    return data;
  },

  dataOut: function __dataOut(data, mongodb) {
    console.log('dataOut', data);
  }
};
