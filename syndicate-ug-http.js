#!/bin/env node
/*
   Copyright 2016 The Trustees of University of Arizona

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
var filter = require('./ipfilter.js');
var express = require('express');
var nodeCache = require('node-cache');
var app = express();

(function main() {
    utils.log_info("Syndicate-UG-HTTP start");

    var args = process.argv.slice(1);
    var param = utils.parse_args(args);

    try {
        // read whitelist
        whitelist = filter.get_white_list();
        express_filter = filter.get_express_filter(whitelist);
        
        if(express_filter == null) {
            // do not filter
            utils.log_info("accept requests from all hosts");
        } else {
            utils.log_info("accept requests from : " + whitelist);

            // filter ip range
            app.use(express_filter);
        }

        // boot up
        var ug = rest.init(param);
        var rfdCache = new nodeCache({
            stdTTL: 600,
            checkperiod: 600,
            useClones: false
        });

        rfdCache.on("expired", function(key, fh) {
            utils.log_debug("closing expired file handle for read - " + key);
            rest.safeclose(ug, fh);
        });

        var wfdCache = new nodeCache({
            stdTTL: 3600,
            checkperiod: 600,
            useClones: false
        });

        wfdCache.on("expired", function(key, fh) {
            utils.log_debug("closing expired file handle for write - " + key);
            rest.safeclose(ug, fh);
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
