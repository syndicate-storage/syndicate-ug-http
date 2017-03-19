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

/**
 * Expose root class
 */
module.exports = {
    is_json_string: function(str) {
        try {
            JSON.parse(str);
        } catch (e) {
            return false;
        }
        return true;
    },
    log_debug: function(str) {
        console.log(util.format("SYNDICATE_UG_HTTP_CLI:DEBUG] %s", str));
    },
    log_error: function(str) {
        console.error(util.format("SYNDICATE_UG_HTTP_CLI:ERROR] %s", str));
    },
    log_info: function(str) {
        console.log(util.format("SYNDICATE_UG_HTTP_CLI:INFO] %s", str));
    }
};
