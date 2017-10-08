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
var restUtils = require('./rest_utils.js');
var utils = require('./utils.js');
var sessionTable = require('./session_table.js');
var syndicate = require('syndicate-storage');


function fs_statvfs(ug, callback) {
    utils.log_debug("fs_statvfs: calling syndicate.statvfs_async");
    try {
        syndicate.statvfs_async(ug, function(err, statvfs) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, statvfs);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_stat(ug, path, callback) {
    utils.log_debug(util.format("fs_stat: calling syndicate.stat_raw_async - %s", path));
    try {
        syndicate.stat_raw_async(ug, path, function(err, stat) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, stat);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_listdir(ug, path, callback) {
    utils.log_debug(util.format("fs_listdir: calling syndicate.list_dir_async - %s", path));
    try {
        syndicate.list_dir_async(ug, path, function(err, entries) {
            if(err) {
                callback(err, null);
                return;
            }

            var json_obj = {
                entries: entries
            };

            callback(null, json_obj);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_getxattr(ug, path, key, callback) {
    utils.log_debug(util.format("fs_getxattr: calling syndicate.get_xattr_async - %s, %s", path, key));
    try {
        syndicate.get_xattr_async(ug, path, key, function(err, xattr) {
            if(err) {
                callback(err, null);
                return;
            }

            var json_obj = {
                value: xattr
            };

            callback(null, json_obj);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_listxattr(ug, path, callback) {
    utils.log_debug(util.format("fs_listxattr: calling syndicate.list_xattr_async - %s", path));
    try {
        syndicate.list_xattr_async(ug, path, function(err, xattrs) {
            if(err) {
                callback(err, null);
                return;
            }

            var json_obj = {
                keys: xattrs
            };

            callback(null, json_obj);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_read_stateless(ug, path, offset, len, callback) {
    utils.log_debug(util.format("fs_read_stateless: calling syndicate.open_async - %s", path));
    try {
        syndicate.open_async(ug, path, 'r', function(err, fh) {
            if(err) {
                callback(err, null);
                return;
            }

            if(offset !== 0) {
                utils.log_debug(util.format("fs_read_stateless: calling syndicate.seek_async - %d", offset));
                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    if(new_offset !== offset) {
                        callback(null, new Buffer(0));
                        return;
                    }

                    utils.log_debug(util.format("fs_read_stateless: calling syndicate.read_async - %d", len));
                    syndicate.read_async(ug, fh, len, function(err, buffer) {
                        if(err) {
                            callback(err, null);
                            return;
                        }

                        utils.log_debug("fs_read_stateless: calling syndicate.close_async");
                        syndicate.close_async(ug, fh, function(err, data) {
                            if(err) {
                                callback(err, null);
                                return;
                            }

                            callback(null, buffer);
                            return;
                        });
                    });
                });
            } else {
                utils.log_debug("fs_read_stateless: calling syndicate.read_async");
                syndicate.read_async(ug, fh, len, function(err, buffer) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    utils.log_debug("fs_read_stateless: calling syndicate.close_async");
                    syndicate.close_async(ug, fh, function(err, data) {
                        if(err) {
                            callback(err, null);
                            return;
                        }

                        callback(null, buffer);
                        return;
                    });
                });
            }
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_read_stateful(ug, path, stat, offset, len, callback) {
    utils.log_debug("fs_read_stateful: calling syndicate.tell");
    try {
        var cur_off = syndicate.tell(ug, stat.fh);
        if(cur_off !== offset) {
            utils.log_debug(util.format("fs_read_stateful: calling syndicate.seek_async - %d, current %d", offset, cur_off));
            syndicate.seek_async(ug, stat.fh, offset, function(err, new_offset) {
                if(err) {
                    callback(err, null);
                    return;
                }

                if(new_offset !== offset) {
                    callback(null, new Buffer(0));
                    return;
                }

                utils.log_debug(util.format("fs_read_stateful: calling syndicate.read_async - %d", len));
                syndicate.read_async(ug, stat.fh, len, function(err, buffer) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    callback(null, buffer);
                    return;
                });
            });
        } else {
            utils.log_debug(util.format("fs_read_stateful: calling syndicate.read_async - %d", len));
            syndicate.read_async(ug, stat.fh, len, function(err, buffer) {
                if(err) {
                    callback(err, null);
                    return;
                }

                callback(null, buffer);
                return;
            });
        }
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_open(ug, gateway_state, path, flag, callback) {
    utils.log_debug(util.format("fs_open: calling syndicate.open_async - %s, %s", path, flag));
    try {
        syndicate.open_async(ug, path, flag, function(err, fh) {
            if(err) {
                callback(err, null);
                return;
            }

            gateway_state.open_fd(path, fh, flag, function(err, fd) {
                if(err) {
                    callback(err, null);
                    return;
                }

                var json_obj = {
                    fd: fd
                };

                callback(null, json_obj);
                return;
            });
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_mkdir(ug, path, mode, callback) {
    utils.log_debug(util.format("fs_mkdir: calling syndicate.mkdir_async - %s, %s", path, mode));
    try {
        syndicate.mkdir_async(ug, path, mode, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, null);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_setxattr(ug, path, key, val, callback) {
    utils.log_debug(util.format("fs_setxattr: calling syndicate.set_xattr_async - %s, %s", path, key));
    try {
        syndicate.set_xattr_async(ug, path, key, val, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, null);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_write_stateless(ug, path, offset, len, req, callback) {
    try {
        var buffer = new Buffer(len);
        var buffer_offset = 0;

        req.on('error', function(err) {
            callback(err, null);
            return;
        });

        req.on('data', function(chunk) {
            chunk.copy(buffer, buffer_offset);
            buffer_offset += chunk.length;
        });

        utils.log_debug(util.format("fs_write_stateless: calling syndicate.open_async - %s", path));
        syndicate.open_async(ug, path, 'w', function(err, fh) {
            if(err) {
                callback(err, null);
                return;
            }

            if(offset !== 0) {
                utils.log_debug(util.format("fs_write_stateless: calling syndicate.seek_async - %d", offset));
                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    if(new_offset !== offset) {
                        callback("can't seek to requested offset", null);
                        return;
                    }

                    req.on('end', function() {
                        utils.log_debug("fs_write_stateless: calling syndicate.write_async");
                        syndicate.write_async(ug, fh, buffer, function(err, data) {
                            if(err) {
                                callback(err, null);
                                return;
                            }

                            utils.log_debug("fs_write_stateless: calling syndicate.fsync_async");
                            syndicate.fsync_async(ug, fh, function(err, data) {
                                if(err) {
                                    callback(err, null);
                                    return;
                                }

                                utils.log_debug("fs_write_stateless: calling syndicate.close_async");
                                syndicate.close_async(ug, fh, function(err, data) {
                                    if(err) {
                                        callback(err, null);
                                        return;
                                    }

                                    callback(null, null);
                                    return;
                                });
                            });
                        });
                    });
                });
            } else {
                req.on('end', function() {
                    utils.log_debug("fs_write_stateless: calling syndicate.write_async");
                    syndicate.write_async(ug, fh, buffer, function(err, data) {
                        if(err) {
                            callback(err, null);
                            return;
                        }

                        utils.log_debug("fs_write_stateless: calling syndicate.fsync_async");
                        syndicate.fsync_async(ug, fh, function(err, data) {
                            if(err) {
                                callback(err, null);
                                return;
                            }

                            utils.log_debug("fs_write_stateless: calling syndicate.close_async");
                            syndicate.close_async(ug, fh, function(err, data) {
                                if(err) {
                                    callback(err, null);
                                    return;
                                }

                                callback(null, null);
                                return;
                            });
                        });
                    });
                });
            }
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_write_stateful(ug, path, stat, offset, len, req, callback) {
    try {
        var buffer = new Buffer(len);
        var buffer_offset = 0;

        req.on('error', function(err) {
            callback(err, null);
            return;
        });

        req.on('data', function(chunk) {
            chunk.copy(buffer, buffer_offset);
            buffer_offset += chunk.length;
        });

        req.on('end', function() {
            utils.log_debug("fs_write_stateful: calling syndicate.tell");
            var cur_off = syndicate.tell(ug, stat.fh);
            if(cur_off !== offset) {
                utils.log_debug(util.format("fs_write_stateful: calling syndicate.seek_async - %d, current %d", offset, cur_off));
                syndicate.seek_async(ug, stat.fh, offset, function(err, new_offset) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    if(new_offset !== offset) {
                        callback("can't seek to requested offset", null);
                        return;
                    }

                    utils.log_debug("fs_write_stateful: calling syndicate.write_async");
                    syndicate.write_async(ug, stat.fh, buffer, function(err, data) {
                        if(err) {
                            callback(err, null);
                            return;
                        }

                        callback(null, null);
                        return;
                    });
                });
            } else {
                utils.log_debug("fs_write_stateful: calling syndicate.write_async");
                syndicate.write_async(ug, stat.fh, buffer, function(err, data) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    callback(null, null);
                    return;
                });
            }
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_rename(ug, path, to_name, callback) {
    utils.log_debug(util.format("fs_rename: calling syndicate.rename_async - %s to %s", path, to_name));
    try {
        syndicate.rename_async(ug, path, to_name, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, null);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_rmdir(ug, path, callback) {
    utils.log_debug(util.format("fs_rmdir: calling syndicate.rmdir_async - %s", path));
    try {
        syndicate.rmdir_async(ug, path, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, null);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_unlink(ug, path, callback) {
    utils.log_debug(util.format("fs_unlink: calling syndicate.unlink_async - %s", path));
    try {
        syndicate.unlink_async(ug, path, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, null);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_rmxattr(ug, path, key, callback) {
    utils.log_debug(util.format("fs_rmxattr: calling syndicate.remove_xattr_async - %s, %s", path, key));
    try {
        syndicate.remove_xattr_async(ug, path, key, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, null);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function fs_close(ug, gateway_state, stat, callback) {
    try {
        gateway_state.close_fd(stat.fd, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            // write mode
            if(stat.flag == 'w') {
                utils.log_debug("fs_close: calling syndicate.fsync_async");
                syndicate.fsync_async(ug, stat.fh, function(err, data) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    utils.log_debug("fs_close: calling syndicate.close_async");
                    syndicate.close_async(ug, stat.fh, function(err, data) {
                        if(err) {
                            callback(err, null);
                            return;
                        }

                        callback(null, null);
                        return;
                    });
                });
            } else {
                utils.log_debug("fs_close: calling syndicate.close_async");
                syndicate.close_async(ug, stat.fh, function(err, data) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    callback(null, null);
                    return;
                });
            }
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}


/**
 * Expose root class
 */
module.exports = {
    get_handler: function(req, res) {
        var options = req.query;
        var path = req.target;
        var session_table = req.session_table;
        var session_name = req.user.name;

        if(!path.startsWith("/fs")) {
            restUtils.return_badrequest(req, res, util.format("path does not have correct /fs prefix - %s", path));
            return;
        }
        // cut off /fs part
        path = path.slice(3);
        // for /fs/?op=val request, /fs?op=val comes
        if(path[0] !== "/") {
            path = "/";
        }

        session_table.get_state(session_name, function(err, gateway_state) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            if(gateway_state === null) {
                // session not exist
                restUtils.return_error(req, res, util.format("cannot retrieve a gateway state - %s", session_name));
                return;
            }

            var ug = gateway_state.ug;
            var return_rest_data = function(err, data) {
                if(err) {
                    restUtils.return_error(req, res, err);
                    return;
                }

                restUtils.return_data(req, res, data);
                return;
            };

            if(options.statvfs !== undefined) {
                // statvfs: ?statvfs
                fs_statvfs(ug, return_rest_data);
            } else if(options.stat !== undefined) {
                // stat: ?stat
                fs_stat(ug, path, return_rest_data);
            } else if(options.listdir !== undefined) {
                // listdir: ?listdir
                fs_listdir(ug, path, return_rest_data);
            } else if(options.getxattr !== undefined) {
                // getxattr: ?getxattr&key='name'
                var key = options.key;
                fs_getxattr(ug, path, key, return_rest_data);
            } else if(options.listxattr !== undefined) {
                // listxattr: ?listxattr
                fs_listxattr(ug, path, return_rest_data);
            } else if(options.read !== undefined) {
                // read: ?read&fd=fd&offset=offset&len=len
                var offset = Number(options.offset) || 0;
                var len = Number(options.len) || 0;
                if(options.fd === undefined) {
                    // stateless
                    fs_read_stateless(ug, path, offset, len, return_rest_data);
                } else {
                    // stateful
                    var fd = options.fd;
                    gateway_state.stat_fd(fd, function(err, stat) {
                        if(err) {
                            restUtils.return_error(req, res, err);
                            return;
                        }

                        if(stat === null) {
                            restUtils.return_error(req, res, util.format("cannot retrieve stat for file handle %d", fd));
                            return;
                        }

                        if(stat.flag !== 'r') {
                            restUtils.return_error(req, res, util.format("file handle %d is not for read", fd));
                            return;
                        }

                        fs_read_stateful(ug, path, stat, offset, len, return_rest_data);
                    });
                }
            } else if(options.open !== undefined) {
                // open: ?open&flag='r'
                var flag = options.flag || 'r';
                fs_open(ug, gateway_state, path, flag, return_rest_data);
            } else if(options.checkopen !== undefined) {
                // checkopen: ?checkopen&fd=fd
                if(options.fd === undefined) {
                    restUtils.return_error(req, res, "fd is not given");
                    return;
                }

                var fd = options.fd;
                gateway_state.stat_fd(fd, function(err, stat) {
                    if(err) {
                        restUtils.return_error(req, res, err);
                        return;
                    }

                    var json_obj = {
                        'opened': stat === null ? false : true
                    };

                    restUtils.return_data(req, res, json_obj);
                    return;
                });
            } else {
                res.status(403).send();
            }
        });
    },
    post_handler: function(req, res) {
        var options = req.query;
        var path = req.target;
        var session_table = req.session_table;
        var session_name = req.user.name;

        if(!path.startsWith("/fs")) {
            restUtils.return_badrequest(req, res, util.format("path does not have correct /fs prefix - %s", path));
            return;
        }
        // cut off /fs part
        path = path.slice(3);
        // for /fs/?op=val request, /fs?op=val comes
        if(path[0] !== "/") {
            path = "/";
        }

        session_table.get_state(session_name, function(err, gateway_state) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            if(gateway_state === null) {
                // session not exist
                restUtils.return_error(req, res, util.format("cannot retrieve a gateway state - %s", session_name));
                return;
            }

            var ug = gateway_state.ug;
            var return_rest_data = function(err, data) {
                if(err) {
                    restUtils.return_error(req, res, err);
                    return;
                }

                restUtils.return_data(req, res, data);
                return;
            };

            if(options.mkdir !== undefined) {
                // mkdir: ?mkdir&mode=777
                var mode = options.mode;
                fs_mkdir(ug, path, mode, return_rest_data);
            } else if(options.setxattr !== undefined) {
                // setxattr: ?setxattr&key='name'&value='value'
                var key = options.key;
                var value = options.value;
                fs_setxattr(ug, path, key, value, return_rest_data);
            } else if(options.write !== undefined) {
                // write: ?write&fd=fd&offset=offset&len=len
                var offset = Number(options.offset) || 0;
                var len = Number(options.len) || 0;

                if(options.fd === undefined) {
                    // stateless
                    fs_write_stateless(ug, path, offset, len, req, return_rest_data);
                } else {
                    // stateful
                    var fd = options.fd;
                    gateway_state.stat_fd(fd, function(err, stat) {
                        if(err) {
                            restUtils.return_error(req, res, err);
                            return;
                        }

                        if(stat === null) {
                            restUtils.return_error(req, res, util.format("cannot retrieve stat for file handle %d", fd));
                            return;
                        }

                        if(stat.flag !== 'w') {
                            restUtils.return_error(req, res, util.format("file handle %d is not for write", fd));
                            return;
                        }

                        fs_write_stateful(ug, path, stat, offset, len, req, return_rest_data);
                    });
                }
            } else if(options.extendttl !== undefined) {
                // extendttl: ?extendttl&fd=fd
                if(options.fd === undefined) {
                    restUtils.return_error(req, res, "fd is not given");
                    return;
                }

                var fd = options.fd;
                gateway_state.extend_fd_ttl(fd, function(err, data) {
                    if(err) {
                        restUtils.return_error(req, res, err);
                        return;
                    }

                    restUtils.return_data(req, res, null);
                    return;
                });
            } else if(options.rename !== undefined) {
                // rename: ?rename&to='to_filename'
                var to_name = options.to;
                fs_rename(ug, path, to_name, return_rest_data);
            } else {
                res.status(403).send();
            }
        });
    },
    delete_handler: function(req, res) {
        var options = req.query;
        var path = req.target;
        var session_table = req.session_table;
        var session_name = req.user.name;

        if(!path.startsWith("/fs")) {
            restUtils.return_badrequest(req, res, util.format("path does not have correct /fs prefix - %s", path));
            return;
        }
        // cut off /fs part
        path = path.slice(3);
        // for /fs/?op=val request, /fs?op=val comes
        if(path[0] !== "/") {
            path = "/";
        }

        session_table.get_state(session_name, function(err, gateway_state) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            if(gateway_state === null) {
                // session not exist
                restUtils.return_error(req, res, util.format("cannot retrieve a gateway state - %s", session_name));
                return;
            }

            var ug = gateway_state.ug;
            var return_rest_data = function(err, data) {
                if(err) {
                    restUtils.return_error(req, res, err);
                    return;
                }

                restUtils.return_data(req, res, data);
                return;
            };

            if(options.rmdir !== undefined) {
                // rmdir: ?rmdir
                fs_rmdir(ug, path, return_rest_data);
            } else if(options.unlink !== undefined) {
                // unlink: ?unlink
                fs_unlink(ug, path, return_rest_data);
            } else if(options.rmxattr !== undefined) {
                // rmxattr: ?rmxattr&key='name'
                var key = options.key;
                fs_rmxattr(ug, path, key, return_rest_data);
            } else if(options.close !== undefined) {
                // close: ?close&fd=fd
                var fd = options.fd;

                gateway_state.stat_fd(fd, function(err, stat) {
                    if(err) {
                        restUtils.return_error(req, res, err);
                        return;
                    }

                    if(stat === null) {
                        restUtils.return_error(req, res, util.format("cannot retrieve stat for file handle %d", fd));
                        return;
                    }

                    fs_close(ug, gateway_state, stat, return_rest_data);
                });
            } else {
                res.status(403).send();
            }
        });
    }
};
