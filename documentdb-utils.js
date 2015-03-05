/*jshint node:true */
"use strict";

// Common utility functions to work with DocumentDB.
var DocDBUtils = {
  getOrCreateDatabase: function (client, databaseId, callback) {
    var query = {
      query: 'SELECT * FROM root r WHERE r.id=@id',
      parameters: [{name: '@id', value: databaseId}]
    };

    client.queryDatabases(query).toArray(function (err, results) {
      if (err) {
        console.log(err);
      } else if (results.length === 0) {
        client.createDatabase({id: databaseId}, function (err, created) {
          if (callback) {
            callback(null, created);
          }
        });
      } else if (callback) {
        callback(null, results[0]);
      }
    });
  },

  getOrCreateCollection: function (client, databaseLink, collectionId, callback) {
    var query = {
      query: 'SELECT * FROM root r WHERE r.id=@id',
      parameters: [{name: '@id', value: collectionId}]
    };

    client.queryCollections(databaseLink, query).toArray(function (err, results) {
      if (err) {
        console.log(err);
      } else if (results.length === 0) {
        client.createCollection(databaseLink, {id: collectionId}, function (err, created) {
          if (callback) {
            callback(null, created);
          }
        });
      } else if (callback) {
        callback(null, results[0]);
      }
    });
  }
};

module.exports = DocDBUtils;