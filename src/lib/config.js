function GetAppConfig() {
    return require(`../../config.${process.env.NODE_ENV}.json`);
  }
  
  module.exports = new GetAppConfig();
      