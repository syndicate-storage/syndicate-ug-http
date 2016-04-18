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
var cluster = require('cluster');
var express = require('express');
var nodeCache = require('node-cache')
var app = express();

(function main() {
    if (cluster.isMaster) {
        console.log("Syndicate-UG-HTTP start");
        var cpuCount = require('os').cpus().length;
        
        //for(var i=0;i<cpuCount;i++){
            cluster.fork();
        //}
    } else {
        var args = process.argv.slice(1);
        var param = utils.parse_args(args);

        try {
            // start restfs
            app.use(function(req, res, next) {
                console.log('%s %s', req.method, req.url);
                next();
            });

            var ug = rest.init(param);
            var rfdCache = new nodeCache({
                stdTTL: 600,
                checkperiod: 600,
                useClones: false
            });

            rfdCache.on("expired", function(key, value) {
                console.log("closing expired file handle for read - " + key);
                rest.safeclose(ug, value);
            });

            var wfdCache = new nodeCache({
                stdTTL: 3600,
                checkperiod: 600,
                useClones: false
            });

            rfdCache.on("expired", function(key, value) {
                console.log("closing expired file handle for read - " + key);
                rest.safeclose(ug, value);
            });

            app.use(function(req, res, next) {
                req.ug = ug;
                req.rfdCache = rfdCache;
                req.wfdCache = wfdCache;
                next();
            });

            app.use('/', rest.getRouter());

            app.listen(param.port, function() {
                console.log("listening at " + param.port);
                console.log("Worker " + cluster.worker.id + " running");
            });

            // must not shutdown here!
            //rest.shutdown(ug);
        } catch (e) {
            console.log("Exception occured: " + e);
        }
    }
})();
