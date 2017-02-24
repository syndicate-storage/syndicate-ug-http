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

var path = require('path');
var fs = require('fs');
var mkdirp = require('mkdirp');

var deleteFolderRecursivelySync = function(path) {
    if(fs.existsSync(path)) {
        fs.readdirSync(path).forEach(function(file,index) {
            var curPath = path + "/" + file;
            if(fs.lstatSync(curPath).isDirectory()) { 
                // recurse
                deleteFolderRecursivelySync(curPath);
            } else { 
                // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
};

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
        console.log(util.format("SYNDICATE_REST:DEBUG] %s", str));
    },
    log_error: function(str) {
        console.error(util.format("SYNDICATE_REST:ERROR] %s", str));
    },
    log_info: function(str) {
        console.log(util.format("SYNDICATE_REST:INFO] %s", str));
    },
    resolve_home: function(filepath) {
        if(filepath[0] === '~') {
            return path.join(process.env.HOME, filepath.slice(1));
        }
        return filepath;
    },
    create_dir_recursively_sync: function(dir, mode) {
        mkdirp.sync(dir, mode);
    },
    create_dir_recursively: function(dir, mode, callback) {
        mkdirp(dir, mode, callback);
    },
    remove_dir_recursively_sync: function(dir) {
        deleteFolderRecursivelySync(dir);
    },
};
