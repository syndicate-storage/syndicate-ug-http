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
var utils = require('./utils.js');
var hadoopCredentials = require('./hadoop_credentials.js');
var minimist = require('minimist');
var username = require('username');

function parse_args(args) {
    var options = {
        session_name: "",
        session_key: ""
    };

    // skip first two args
    // 1: node
    // 2: *.js script
    var argv = minimist(args.slice(2));

    // parse
    options.session_name = argv.session_name || "";
    options.session_key = argv.session_key || "";
    return options;
}

function check_config(conf) {
    if(conf.session_name && conf.session_key) {
        return true;
    }
    return false;
}

(function main() {
    utils.log_info("Setup a hadoop credential");

    var param = parse_args(process.argv);
    
    try {
        if(!check_config(param)) {
            throw new Error("arguments are not given properly");
        }

        if(hadoopCredentials.is_hadoop_available()) {
            var hadoop_user = username.sync();
            hadoopCredentials.setup_hadoop_credential(hadoop_user, param.session_name, param.session_key, function(err, data) {
                if(err) {
                    utils.log_error(util.format("Could not setup a hadoop credential: %s", err));
                    return;
                }
                utils.log_info(util.format("Successfully setup a hadoop credential: %s", param.session_name));
            });
        } else {
            utils.log_info("Hadoop is not accessible - ignoring setting up a hadoop credential");
        }
    } catch (e) {
        utils.log_error(util.format("Exception occured: %s", e));
        process.exit(1);
    }
})();
