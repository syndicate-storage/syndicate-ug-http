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
var path = require('path');
var fs = require('fs');
var mkdirp = require('mkdirp');
var crypto = require('crypto');
var temp = require('temp');

// Automatically track and cleanup files at exit
temp.track();

function delete_folder_sync(path) {
    if(fs.existsSync(path)) {
        fs.readdirSync(path).forEach(function(file, index) {
            var curPath = path + "/" + file;
            if(fs.lstatSync(curPath).isDirectory()) {
                // recurse
                delete_folder(curPath);
            } else {
                // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
};

function delete_folder(path, callback) {
    fs.stat(path, function(err, stats) {
        if(err) {
            callback(err,stats);
            return;
        }

        if(stats.isFile()) {
            fs.unlink(path, function(err) {
                if(err) {
                    callback(err, null);
                } else {
                    callback(null, true);
                }
                return;
            });
        } else if(stats.isDirectory()) {
            fs.readdir(path, function(err, files) {
                if(err) {
                    callback(err, null);
                    return;
                }

                var f_length = files.length;
                var f_delete_index = 0;

                var checkStatus = function() {
                    if(f_length === f_delete_index) {
                        fs.rmdir(path, function(err) {
                            if(err) {
                                callback(err, null);
                            } else {
                                callback(null, true);
                            }
                        });
                        return true;
                    }

                    return false;
                };

                if(!checkStatus()) {
                    for(var i=0;i<f_length;i++) {
                        (function() {
                            var filePath = path + '/' + files[i];
                            delete_folder(filePath, function removeRecursiveCB(err, status) {
                                if(!err){
                                    f_delete_index++;
                                    checkStatus();
                                } else {
                                    callback(err, null);
                                    return;
                                }
                            });
                        })();
                    }
                }
            });
        }
    });
}

function write_temp_file(bytes, callback) {
    temp.open({prefix: "syn_ug_http_", suffix: '.tmp'}, function(err, info) {
        if(err) {
            callback(err, null);
            return;
        }

        fs.write(info.fd, bytes, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            fs.close(info.fd, function(err) {
                if(err) {
                    callback(err, null);
                    return;
                }

                callback(null, info.path);
                return;
            });
        });
    });
}


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
    get_absolute_path: function(filepath) {
        if(filepath[0] === '~') {
            return path.join(process.env.HOME, filepath.slice(1));
        } else if(filepath[0] === '.') {
            return path.resolve(filepath);
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
        delete_folder_sync(dir);
    },
    remove_dir_recursively: function(dir, callback) {
        delete_folder(dir, callback);
    },
    check_existance_sync: function(dir) {
        return fs.existsSync(dir);
    },
    check_existance: function(dir, callback) {
        fs.exists(dir, function(exist) {
            callback(null, exist);
        });
    },
    generate_random_string: function(bytes) {
        return crypto.randomBytes(bytes).toString('hex');
    },
    generate_checksum: function(bytes) {
        var shasum = crypto.createHash('sha256');
        shasum.update(bytes);
        return shasum.digest('hex');
    },
    write_temp_file: function(bytes, callback) {
        write_temp_file(bytes, callback);
    }
};
