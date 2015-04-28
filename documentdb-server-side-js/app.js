/*jshint node:true */
"use strict";

var DocumentDBClient = require('documentdb').DocumentClient;
var DocumentDBUtils = require('./docdb-utils');
var config = require('./config');

// Cache Database and Collection self-links.
var databaseLink, collectionLink, sprocLink;

// Initialize DocumentDB Client.
var docDbClient = new DocumentDBClient(config.host, {masterKey: config.authKey});

/////////////////////////////////////////////////////////////////////////////////////
// This is common boilerplate... we'll put all the interesting logic in main() below.
/////////////////////////////////////////////////////////////////////////////////////

// Get or Create the Database
DocumentDBUtils.getOrCreateDatabase(docDbClient, config.databaseId, function (err, db) {
    databaseLink = db._self;

    // Get or Create the Collection
    DocumentDBUtils.getOrCreateCollection(docDbClient, databaseLink, config.collectionId, function (err, coll) {
        collectionLink = coll._self;

        // Call the meat.
        main();
    });
});

//////////////////////
// The meat goes here.
//////////////////////
var sampleDocument = {
    id: "heather",
    health: 20,
    level: 40,
    item: {
        id: "iron_sword",
        name: "Iron Sword",
        type: "sword",
        level: 4,
        damage: 7
    },
    money: 9000
};

var sprocDefinition = require('./stored-procedures/bulkDelete');
var sprocParams = ["SELECT * FROM docs WHERE docs.founded_year = 2000"];

function main() {
    DocumentDBUtils.upsertSproc(docDbClient, collectionLink, sprocDefinition, function(err, sproc) {
        if (err) throw err;

        executeSprocContinuation(docDbClient, sproc, sprocParams, null, function(err, results, responseHeaders) {
            console.log('done.');
        });
    });
}

function executeSprocContinuation(docDbClient, sproc, sprocParams, continuation, callback) {
    DocumentDBUtils.executeSproc(docDbClient, sproc._self, sprocParams, function(err, results, responseHeaders) {

        console.log('//////////////////////////////////');
        console.log('// sprocParams');
        console.log(sprocParams);
        console.log('// err');
        console.log(err);
        if(responseHeaders) {
            console.log('// responseHeaders');
            console.log(responseHeaders);
        }
        console.log('// results');
        console.log(results);
        console.log('//////////////////////////////////');

        if(err && err.code === 429 && responseHeaders['x-ms-retry-after-ms']) {
            console.log('retrying...');
            setTimeout(function() {
                executeSprocContinuation(docDbClient, sproc, sprocParams, continuation, callback);
            }, responseHeaders['x-ms-retry-after-ms']);
        } else if(results && results.continuation) {
            console.log('continuing...');
            executeSprocContinuation(docDbClient, sproc, sprocParams, continuation, callback);
        } else {
            console.log('done...');
            callback(err, results, responseHeaders);
        }
    });
}
