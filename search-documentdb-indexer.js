var request = require("request");

var Search = function (serviceName, apiKey) {
    this.serviceName = serviceName;
    this.apiKey = apiKey;
    this.apiVersion = "2015-02-28";
};

Search.prototype.createDataSource = function (dataSourceName, docDbEndpoint, docDbKey, docDbDatabase, docDbCollection, callback) {
    request({
            method: "PUT",
            uri: "https://" + this.serviceName + ".search.windows.net/datasources/" + dataSourceName + "?api-version=" + this.apiVersion,
            headers: {
                "Content-Type": "application/json",
                "api-key": this.apiKey
            },
            json: {
                "name": dataSourceName,
                "type": "documentdb",
                "credentials": {
                    "connectionString": "AccountEndpoint=" + docDbEndpoint + ";AccountKey=" + docDbKey + ";Database=" + docDbDatabase
                },
                "container": {
                    "name": docDbCollection
                },
                "dataChangeDetectionPolicy": {
                    "@odata.type": "#Microsoft.Azure.Search.HighWaterMarkChangeDetectionPolicy",
                    "highWaterMarkColumnName": "_ts"

                },
                "dataDeletionDetectionPolicy": {
                    "@odata.type": "#Microsoft.Azure.Search.SoftDeleteColumnDeletionDetectionPolicy",
                    "softDeleteColumnName": "IsDeleted",
                    "softDeleteMarkerValue": "true"
                }
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.listDataSources = function (callback) {
    request({
            method: "GET",
            uri: "https://" + this.serviceName + ".search.windows.net/datasources?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.deleteDataSource = function (dataSourceName, callback) {
    request({
            method: "DELETE",
            uri: "https://" + this.serviceName + ".search.windows.net/datasources/" + dataSourceName + "?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.createIndexer = function (indexerName, dataSourceName, targetIndexName, callback) {
    request({
            method: "PUT",
            uri: "https://" + this.serviceName + ".search.windows.net/indexers/" + indexerName + "?api-version=" + this.apiVersion,
            headers: {
                "Content-Type": "application/json",
                "api-key": this.apiKey
            },
            json: {
                name: indexerName,
                dataSourceName: dataSourceName,
                targetIndexName: targetIndexName,
                schedule: {interval: "PT5M", "startTime": "2015-01-01T00:00:00Z"}
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};


Search.prototype.listIndexers = function (callback) {
    request({
            method: "GET",
            uri: "https://" + this.serviceName + ".search.windows.net/indexers?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.deleteIndexer = function (indexerName, callback) {
    request({
            method: "DELETE",
            uri: "https://" + this.serviceName + ".search.windows.net/indexers/" + indexerName + "?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.runIndexer = function (indexerName, callback) {
    request({
            method: "POST",
            uri: "https://" + this.serviceName + ".search.windows.net/indexers/" + indexerName + "/run?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.getIndexerStatus = function (indexerName, callback) {
    request({
            method: "GET",
            uri: "https://" + this.serviceName + ".search.windows.net/indexers/" + indexerName + "/status?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.resetIndexer = function (indexerName, callback) {
    request({
            method: "POST",
            uri: "https://" + this.serviceName + ".search.windows.net/indexers/" + indexerName + "/reset?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.loadDocuments = function (targetIndexName, documents, callback) {
    request({
            method: "POST",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes/" + targetIndexName + "/docs/index?api-version=" + this.apiVersion,
            headers: {
                "Content-Type": "application/json",
                "api-key": this.apiKey
            },
            json: {
                value: documents
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.loadDocuments = function (targetIndexName, documents, callback) {
    request({
            method: "POST",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes/" + targetIndexName + "/docs/index?api-version=" + this.apiVersion,
            headers: {
                "Content-Type": "application/json",
                "api-key": this.apiKey
            },
            json: {
                value: documents
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};


Search.prototype.createIndex = function (indexJson, callback) {
    request({
            method: "PUT",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes/" + indexJson.name + "?api-version=" + this.apiVersion,
            headers: {
                "Content-Type": "application/json",
                "api-key": this.apiKey
            },
            json: indexJson
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};


Search.prototype.listIndexes = function (callback) {
    request({
            method: "GET",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.deleteIndex = function (indexName, callback) {
    request({
            method: "DELETE",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes/" + indexName + "?api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.queryIndex = function (indexName, queryString, callback) {
    request({
            method: "GET",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes/" + indexName + "/docs?" + queryString + "&api-version=" + this.apiVersion,
            headers: {
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

Search.prototype.getIndexStats = function (indexName,  callback) {
    request({
            method: "GET",
            uri: "https://" + this.serviceName + ".search.windows.net/indexes/" + indexName + "/stats?api-version=" + this.apiVersion,
            headers: {
                "content-type": "application/json",
                "api-key": this.apiKey
            }
        },
        function (error, response, body) {
            if (error) {
                console.log(error);
            } else {
                console.log(body);
                if (callback) {
                    callback();
                }
            }
        }
    );
};

if (typeof exports !== "undefined") {
    module.exports = Search;
}
