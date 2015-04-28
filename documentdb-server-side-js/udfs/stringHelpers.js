/**
* UDFs for common string operations
* WARNING: use within projections or with other filters to avoid query scans
*/
var stringHelpers = {

    charAt: {
        id: "charAt",
        body: function (input, i) {
            return input.charAt(i);
        }
    },

    indexOf: {
        id: "indexOf",
        body: function (input, token) {
            return input.indexOf(token);
        }
    },

    replace: {
        id: "replace",
        body: function (input, oldValue, newValue) {
            return input.replace(oldValue, newValue);
        }
    },

    toLowerCase: {
        id: "toLowerCase",
        body: function (input) {
            return input.toLowerCase();
        }
    },

    toUpperCase: {
        id: "toUpperCase",
        body: function (input) {
            return input.toLowerCase();
        }
    },

    trim: {
        id: "trim",
        body: function (input) {
            return input.trim();
        }
    },

    length: {
        id: "length",
        body: function (input) {
            return input.length;
        }
    },

    match: {
        id: "match",
        body: function (input, regex) {
            return input.match(regex);
        }
    },

    stringifyJSON: {
        id: "stringifyJSON",
        body: function (input) {
            return JSON.stringify(input);
        }
    },

    parseJSON: {
        id: "parseJSON",
        body: function (input) {
            return JSON.parse(input);
        }
    },

    //# modified slightly from http://stringjs.com/string.js
    camelize: {
        id: "camelize",
        body: function (input) {
            return input.trim().replace(/(\-|_|\s)+(.)?/g, function(mathc, sep, c) {
                return (c ? c.toUpperCase() : '');
            });
        }
    }
};

module.exports = stringHelpers;
