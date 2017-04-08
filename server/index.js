#!/usr/bin/env node
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

var util = require('util');
var rest = require('./rest.js');
var utils = require('./utils.js');
var filter = require('./ipfilter.js');
var serverConfig = require('./server_config.js');
var express = require('express');
var minimist = require('minimist');
var path = require('path');

var app = express();

function get_white_list_path() {
    return util.format("%s/%s", __dirname, "whitelist");
}

function get_default_server_config_path() {
    return util.format("%s/%s", __dirname, "server_config.json");
}

function parse_args(args) {
    var options = {
        debug_level: 0,
        port: 0,
    };

    // skip first two args
    // 1: node
    // 2: *.js script
    var argv = minimist(args.slice(2));

    // parse
    options.debug_level = argv.d || 0;
    if("port" in argv) {
        options.port = argv.port;
    } else if("p" in argv) {
        options.port = argv.p;
    }
    options.config_path = argv.c || get_default_server_config_path();
    
    return options;
}

(function main() {
    utils.log_info("Syndicate-UG-HTTP start");

    var param = parse_args(process.argv);
    var server_config = serverConfig.get_config(param.config_path, param);
    if(server_config == null) {
        utils.log_error("cannot read configuration");
        process.exit(1);
    }

    try {
        // set ip filter
        whitelist = filter.get_white_list(get_white_list_path());
        express_filter = filter.get_express_filter(whitelist);
        
        if(express_filter == null) {
            // do not filter
            utils.log_info("accept requests from all hosts");
        } else {
            utils.log_info(util.format("accept requests from : %s", whitelist));

            // filter ip range
            app.use(express_filter);
        }

        // boot up
        rest.init(app, server_config);
        app.use('/', rest.get_router());

        app.listen(server_config.port, function() {
            utils.log_info(util.format("listening at %d", server_config.port));
        });
    } catch (e) {
        utils.log_error(util.format("Exception occured: %s", e));
    }
})();
