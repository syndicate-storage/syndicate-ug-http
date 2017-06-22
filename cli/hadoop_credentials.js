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
var exec = require('child_process').exec;
var execSync = require('child_process').execSync;

/**
 * Expose root class
 */
module.exports = {
    setup_hadoop_credential: function(hadoop_user, session_name, session_key, callback) {
        /*
            hadoop credential create \
                        fs.hsyndicate.session.session_name.key \
                        -v session_key \
                        -provider jceks://hdfs/user/iychoi/.syndicate/hsyndicate.jceks
        */
        /*
            hadoop dfs -D hadoop.security.credential.provider.path=jceks://hdfs/user/iychoi/.syndicate/hsyndicate.jceks -ls
        */

        utils.log_info(util.format("setting up a hadoop credential for %s", session_name));
        
        var provider_path = util.format("jceks://hdfs/user/%s/.syndicate/hsyndicate.jceks", hadoop_user);
        var name_pattern = util.format("fs.hsyndicate.session.%s.key", session_name);

        var cmd = util.format("hadoop credential create %s -v %s -provider %s", name_pattern, session_key, provider_path);
        utils.log_debug(cmd);
        var child = exec(cmd, function(error, stdout, stderr) {
            if (error) {
                callback(error, null);
                return;
            }
            callback(null, stdout);
        });
    },
    is_hadoop_available: function() {
        var cmd = "hadoop version";
        try {
            var stdout = execSync(cmd);
            return true;
        } catch (e) {
            // exitcode is not 0
            return false;
        }
    }
};
