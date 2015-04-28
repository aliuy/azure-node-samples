/*jshint node:true */
"use strict";

/*
 * General utility methods for interacting with DocumentDB.
 */
var DocDBUtils = {
    // Gets or creates (if it doesn't already exist) a database by id.
    getOrCreateDatabase: function (client, databaseId, callback) {
        var query = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [{name: '@id', value: databaseId}]
        };

        client.queryDatabases(query).toArray(function (err, results) {
            if (err) {
                callback(err);
            } else if (results.length === 0) {
                client.createDatabase({id: databaseId}, callback);
            } else {
                callback(null, results[0]);
            }
        });
    },

    /** 
    * Gets or creates (if it doesn't already exist) a collection by id.
    */
    getOrCreateCollection: function (client, databaseLink, collectionId, callback) {
        var query = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [{name: '@id', value: collectionId}]
        };

        client.queryCollections(databaseLink, query).toArray(function (err, results) {
            if (err) {
                callback(err);
            } else if (results.length === 0) {
                client.createCollection(databaseLink, {id: collectionId}, callback);
            } else {
                callback(null, results[0]);
            }
        });
    },

    upsertSproc: function (client, collectionLink, sprocDefinition, callback) {
        var query = {
            query: 'SELECT * FROM sprocs s WHERE s.id = @id',
            parameters: [{name: '@id', value: sprocDefinition.id}]
        };

        // Query for the stored procedure.
        client.queryStoredProcedures(collectionLink, query, null).toArray(function (err, results) {
            DocDBUtils.handleError(err);

            if (results.length > 0) {
                // Delete it if it exists and re-create it.
                client.deleteStoredProcedure(results[0]._self, null, function(err) { 
                    DocDBUtils.handleError(err);
                    DocDBUtils.createSproc(client, collectionLink, sprocDefinition, callback);
                });
            } else {
                // Otherwise just create the sproc.
                DocDBUtils.createSproc(client, collectionLink, sprocDefinition, callback);
            }
        });
    },

    createSproc: function (client, collectionLink, sprocDefinition, callback) {
        client.createStoredProcedure(collectionLink, sprocDefinition, null, function (err, sproc) {
            DocDBUtils.handleError(err);
            callback(null, sproc);
        });
    },

    executeSproc: function (client, sprocLink, sprocParams, callback) {
        client.executeStoredProcedure(sprocLink, sprocParams, function (err, results, responseHeaders) {
            DocDBUtils.handleError(err);
            callback(err, results, responseHeaders);
        });
    },

    upsertUdf: function (client, collectionLink, udfDefinition, callback) {
        var query = {
            query: 'SELECT * FROM udfs u WHERE u.id = @id',
            parameters: [{name: '@id', value: udfDefinition.id}]
        };

        client.queryUserDefinedFunctions(collectionLink, query, null).toArray(function (err, results) {
            DocDBUtils.handleError(err);

            if (results.length > 0) {
                // Delete it if it exists and re-create it.
                client.deleteUserDefinedFunction(results[0]._self, null, function(err) { 
                    DocDBUtils.handleError(err);
                    DocDBUtils.createUdf(client, collectionLink, udfDefinition, callback);
                });
            } else {
                DocDBUtils.createUdf(client, collectionLink, udfDefinition, callback);
            }
        });
    },

    createUdf: function (client, collectionLink, udfDefinition, callback) {
        client.createUserDefinedFunction(collectionLink, udfDefinition, null, function (err, udf) {
            DocDBUtils.handleError(err);
            callback(null, udf);
        });
    },

    handleError: function (err) {
        if (err) {
            if (err.code === 401) {
                console.log("Check the endpoint and authkey specified in config.js");
            }
        }
    }
};

module.exports = DocDBUtils;