var assert = require("assert")
var DocumentDBClient = require('documentdb').DocumentClient;
var DocumentDBUtils = require('../docdb-utils');
var config = require('../config');
var client = new DocumentDBClient(config.host, {masterKey: config.authKey});
var database, collection, helloWorldSproc;

describe('DocumentDBUtils', function(){
  before(function(done) {
    client.queryDatabases({query: "SELECT * FROM dbs d WHERE d.id = @id", parameters: [{name: "@id", value: config.databaseId}]}).toArray(function (err, dbs) {
        if (err) throw err;
        if (dbs.length == 0) { 
            done();
        } else {
            client.deleteDatabase(dbs[0]._self, function(err) { 
                done();
            });
        }
    });
  })

  describe('#getOrCreateDatabase()', function(){
    it('should create a database when one doesn\'t exist', function(done) {
      DocumentDBUtils.getOrCreateDatabase(client, config.databaseId, function(err, db) {
          assertNoError(err);
          assert(isDefined(db), "db is found");
          assert(isDefined(db._self), "self is not a string");
          database = db;
          done();
      })
    }),
    
    it('should get the database when one already exists', function(done) {
      DocumentDBUtils.getOrCreateDatabase(client, config.databaseId, function(err, db) {
          assertNoError(err);
          assert(isDefined(db), "db is found");
          assert(isDefined(db._self), "self is not a string");
          database = db;
          done();
      })
    }),

    it('should fail with unauthorized with bad keys', function(done) {
      DocumentDBUtils.getOrCreateDatabase(
          new DocumentDBClient(config.host, {masterKey: "wrongKey"}), 
          config.databaseId, 
          function(err, db) {
              assert(isDefined(err), "no err value");
              assert(err.code === 401, "err.code is not 401. was (" + err + ")");
              assert(isNullOrUndefined(db), "db value is unexpectedly returned");
              done();
     })
  })

  describe('#getOrCreateCollection()', function(){
    it('should create a collection when one doesnt exist', function(done) {
      DocumentDBUtils.getOrCreateCollection(client, database._self, config.collectionId, function(err, coll) {
          assertNoError(err);
          assert(isDefined(coll), "coll is found");
          assert(isDefined(coll._self), "self is not a string");

          collection = coll;
          done();
      });
    }),
    it('should get the collection when one already exists', function(done) {
      DocumentDBUtils.getOrCreateCollection(client, database._self, config.collectionId, function(err, coll) {
          assertNoError(err);
          assert(isDefined(coll), "coll is found");
          assert(isDefined(coll._self), "self is not a string");
          done();
      });
    })
  }),

  describe('#createSproc()', function(){
    it('should create a sproc when one doesnt exist', function(done) {
      DocumentDBUtils.createSproc(client, collection._self, 
          {"id": "HelloWorld", "body": function(params) { getContext().getResponse().setBody("Hello World!" + params);}}, 
              function(err, sproc) {
                  assertNoError(err);
                  assert(isDefined(sproc), "sproc is found");
                  assert(isDefined(sproc._self), "self is not a string");

                  helloWorldSproc = sproc;
                  done();
              }
       );
    })
  }),

  describe('#upsertSproc()', function(){
    it('should create a sproc when one doesnt exist', function(done) {
      DocumentDBUtils.upsertSproc(client, collection._self, 
          {"id": "HelloWorld2", "body": function() { getContext().getResponse().setBody("Hello World!");}}, 
              function(err, sproc) {
                  assertNoError(err);
                  assert(isDefined(sproc), "sproc is found");
                  assert(isDefined(sproc._self), "self is not a string");
                  done();
              }
       );
    }),
    it('should update a sproc when it exists', function(done) {
      DocumentDBUtils.upsertSproc(client, collection._self, 
          {"id": "HelloWorld2-updated", "body": function() { getContext().getResponse().setBody("Hello World!");}}, 
              function(err, sproc) {
                  assertNoError(err);
                  assert(isDefined(sproc), "sproc is found");
                  assert(isDefined(sproc._self), "self is not a string");
                  assert(sproc.id == "HelloWorld2-updated", "id not updated");
                  done();
              }
       );
    })
  }),

  describe('#executeSproc()', function(){
    it('should execute Hello World and return results', function(done) {
      DocumentDBUtils.executeSproc(client, helloWorldSproc._self, ["Mocha"], function(err, results) { 
        assertNoError(err);
        assert(results === "Hello World!Mocha");
        done();
      });
    }),
    it('should return error if stored procedure fails', function(done) {
      DocumentDBUtils.executeSproc(client, helloWorldSproc._self, ["Mocha"], function(err, results) { 
        assertNoError(err);
        assert(results === "Hello World!Mocha");
        done();
      });
    })
  })
  })
});

var upsertSproc = require('../stored-procedures/upsert');
var upsertOptimizedForReplaceSproc = require('../stored-procedures/upsert');

describe('Sprocs', function() {
    describe('#upsert', function(){
        before(function(done) {
            DocumentDBUtils.upsertSproc(client, collection._self, upsertSproc, function(err, sproc) {
                assertNoError(err);
                upsertSproc = sproc;
                done();
            });
        })

        it('upsert creates when a document doesn\'t exist', function(done) {
          DocumentDBUtils.executeSproc(client, upsertSproc._self, {id: "newDocument"}, function(err, results) { 
            assertNoError(err);
            assertEquals({op: "created"}, results);
            done();
          });
        }),
        it('upsert updates when a document exists', function(done) {
          DocumentDBUtils.executeSproc(client, upsertSproc._self, {id: "newDocument"}, function(err, results) { 
            assertNoError(err);
            assertEquals({op: "replaced"}, results);
            done();
          });
        }),
        it('upsert auto generates ids when absent', function(done) {
          DocumentDBUtils.executeSproc(client, upsertSproc._self, {}, function(err, results) { 
            assertNoError(err);
            assertEquals({op: "created"}, results);
            done();
          });
        })
    }),

    describe('#upsertOptimizedForReplace', function(){
        before(function(done) {
            DocumentDBUtils.upsertSproc(client, collection._self, upsertOptimizedForReplaceSproc, function(err, sproc) {
                assertNoError(err);
                upsertOptimizedForReplaceSproc = sproc;
                done();
            });
        })

        it('upsert creates when a document doesn\'t exist', function(done) {
          DocumentDBUtils.executeSproc(client, upsertOptimizedForReplaceSproc._self, {id: "newDocument2"}, function(err, results) { 
            assertNoError(err);
            assertEquals({op: "created"}, results);
            done();
          });
        }),
        it('upsert updates when a document exists', function(done) {
          DocumentDBUtils.executeSproc(client, upsertOptimizedForReplaceSproc._self, {id: "newDocument2"}, function(err, results) { 
            assertNoError(err);
            assertEquals({op: "replaced"}, results);
            done();
          });
        }),
        it('upsert auto generates ids when absent', function(done) {
          DocumentDBUtils.executeSproc(client, upsertOptimizedForReplaceSproc._self, {}, function(err, results) { 
            assertNoError(err);
            assertEquals({op: "created"}, results);
            done();
          });
        })
    })
});

var assertNoError = function(err) {
    assert(isNullOrUndefined(err), "err is not null or undefined (" + JSON.stringify(err) + ")");
};

var assertEquals = function(expected, actual) {
    assert(JSON.stringify(expected) == JSON.stringify(actual), "expected: " + JSON.stringify(expected) + ", actual:" + JSON.stringify(actual));
};

var isNullOrUndefined = function(val) {
    return typeof val === "undefined" || val == null;
};

var isNullOrUndefined = function(val) {
    return typeof val === "undefined" || val == null;
};

var isDefined = function(val) {
    return typeof val !== "undefined" ;
};

