#!/bin/env node
/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

var rest = require('./rest.js');
var utils = require('./utils.js');
var express = require('express');
var nodeCache = require('node-cache');
var ipfilter = require('express-ipfilter');
var app = express();
var fs = require('fs');
var rangeCheck = require('range_check');

function extendIPArray(arr) {
    var new_arr = [];
    for(var i=0;i<arr.length;i++) {
        if(typeof(arr[i]) === "string") {
            new_arr.push(arr[i]);
            if(rangeCheck.ver(arr[i]) === 4) {
                var v6ip = "::ffff:" + arr[i].trim();
                if(arr.indexOf(v6ip) < 0) {
                    new_arr.push(v6ip);
                }
            }
        } else if(arr[i] instanceof Array) {
            // ip range
            new_arr.push(arr[i]);
            if(arr[i].length == 2) {
                if(rangeCheck.ver(arr[i][0]) === 4 && rangeCheck.ver(arr[i][1]) === 4) {
                    var v6ip_begin = "::ffff:" + arr[i][0].trim();
                    var v6ip_end = "::ffff:" + arr[i][1].trim();
                    new_arr.push([v6ip_begin, v6ip_end]);
                }
            }
        }
    }
    return new_arr;
}

// read local whilelist
function getWhilelist() {
    var whitelist = "";
    var list = [];

    try {
        whitelist = fs.readFileSync('whitelist', 'utf8');
    } catch (e) {
        // set to default
        whitelist = "ALL";
    }

    if(utils.is_json_string(whitelist)) {
        var json_obj = JSON.parse(whitelist);
        list = json_obj;
    } else {
        list = whitelist.trim().split(/\r?\n/);
    }

    if(list.indexOf("localhost") >= 0 && list.indexOf("127.0.0.1") < 0) {
        list.push("127.0.0.1");
    }

    // handle default ipv6 conversion from ipv4
    return extendIPArray(list);
}

(function main() {
    utils.log_info("Syndicate-UG-HTTP start");

    var args = process.argv.slice(1);
    var param = utils.parse_args(args);

    try {
        // read whitelist
        whitelist = getWhilelist();
        if(whitelist.indexOf("ALL") >= 0) {
            // do not filter
            utils.log_info("accept requests from all hosts");
        } else {
            utils.log_info("accept requests from : " + whitelist);

            // filter ip range
            app.use(ipfilter(whitelist, {mode: 'allow'}));
        }

        // start rest
        app.use(function(req, res, next) {
            //utils.log_info(req.method + " " + req.url);
            next();
        });

        var ug = rest.init(param);
        var rfdCache = new nodeCache({
            stdTTL: 600,
            checkperiod: 600,
            useClones: false
        });

        rfdCache.on("expired", function(key, value) {
            utils.log_debug("closing expired file handle for read - " + key);
            rest.safeclose(ug, value.fh);
        });

        var wfdCache = new nodeCache({
            stdTTL: 3600,
            checkperiod: 600,
            useClones: false
        });

        wfdCache.on("expired", function(key, value) {
            utils.log_debug("closing expired file handle for write - " + key);
            rest.safeclose(ug, value.fh);
        });

        var statistics = {};

        app.use(function(req, res, next) {
            req.ug = ug;
            req.rfdCache = rfdCache;
            req.wfdCache = wfdCache;
            req.statistics = statistics;
            next();
        });

        app.use('/', rest.getRouter());

        app.listen(param.port, function() {
            utils.log_info("listening at " + param.port);
        });

        // must not shutdown here!
        //rest.shutdown(ug);
    } catch (e) {
        utils.log_error("Exception occured: " + e);
    }
})();
