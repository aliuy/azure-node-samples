var config = {}

config.host = process.env.HOST || "[URI for DocumentDB endpoint]";
config.authKey = process.env.AUTH_KEY || "[Master key for DocumentDB]";
config.databaseId = "docdb";
config.collectionId = "scripts";

module.exports = config;
