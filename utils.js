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

/**
 * Expose root class
 */
module.exports = {
    parse_args: function(args) {
        var options = {
            user: "",
            volume: "",
            gateway: "",
            debug_level: 0,
            anonymous: false,
            port: 8888,
        };

        // parse
        // start from 1, [0] is "syndicate-ug-http.js"
        var i;
        for(i=1;i<args.length;i++) {
            if(args[i] === "-u") {
                if(i+1 < args.length) {
                    options.user = args[i+1];
                    i++;
                }
                continue;
            }

            if(args[i] === "-v") {
                if(i+1 < args.length) {
                    options.volume = args[i+1];
                    i++;
                }
                continue;
            }

            if(args[i] === "-g") {
                if(i+1 < args.length) {
                    options.gateway = args[i+1];
                    i++;
                }
                continue;
            }

            if(args[i] === "-d") {
                if(i+1 < args.length) {
                    options.debug_level = parseInt(args[i+1]);
                    i++;
                }
                continue;
            }

            // parse pattern "-d2"
            var debug_level_match = args[i].match(/^(-d)(\d+)/i);
            if(debug_level_match !== null) {
                options.debug_level = parseInt(debug_level_match[2]);
                continue;
            }

            if(args[i] === "--anonymous" || args[i] === "-A") {
                options.anonymous = true;
                continue;
            }

            if(args[i] === "--port" || args[i] === "-p") {
                if(i+1 < args.length) {
                    options.port = parseInt(args[i+1]);
                    i++;
                }
                continue;
            }
        }

        return options;
    }
};
