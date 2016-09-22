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

var express = require('express');
var path = require('path');
var querystring = require('querystring');
var syndicate = require('syndicate-drive');
var utils = require('./utils.js');

var g_fd = 1;

function getNextFileDescriptor(req) {
    var fd = g_fd;
    g_fd++;
    return fd;
}

function getStatistics(req) {
    return req.statistics;
}

var stat = {
    keys: {
        RESPONSE: "response",
        RESPONSE_DATA: "response_data",
        RESPONSE_ERROR: "response_error",
        REQUEST: "request",
        REQUEST_GET: "request_get",
        REQUEST_POST: "request_post",
        REQUEST_DELETE: "request_delete",
        FILE_OPENED: "file_opened",
        FILE_READ: "file_read",
        FILE_WRITE: "file_write",
    },
    get: function(req, key) {
        var val = null;
        statistics = getStatistics(req);
        if(key in statistics) {
            val = statistics[key];
        } else {
            val = 0;
        }
        return val;
    },
    inc: function(req, key) {
        var val = null;
        statistics = getStatistics(req);
        if(key in statistics) {
            val = statistics[key];
        } else {
            val = 0;
        }

        statistics[key] = val+1;
        utils.log_debug("stat_inc " + key + " - " + statistics[key]);
    },
    dec: function(req, key) {
        var val = null;
        statistics = getStatistics(req);
        if(key in statistics) {
            val = statistics[key];
        } else {
            val = 0;
        }

        if(val-1 >= 0) {
            statistics[key] = val - 1;
        }

        utils.log_debug("stat_dec " + key + " - " + statistics[key]);
    },
};

function make_error_object(ex) {
    if(ex instanceof Error) {
        return {
            name: ex.name,
            message: ex.message,
        };
    } else {
        return {
            name: "error",
            message: ex,
        };
    }
}

function return_data(req, res, data) {
    // return with HTTP 200 code
    res.status(200).send(data);
    stat.inc(req, stat.keys.RESPONSE);
    stat.inc(req, stat.keys.RESPONSE_DATA);
    if(data instanceof Buffer) {
        utils.log_info("Respond with data (code 200) > " + data.length + " bytes");
    } else {
        utils.log_info("Respond with data (code 200)");
    }
}

function return_error(req, res, ex) {
    if(ex instanceof syndicate.syndicate_error) {
        if(ex.extra === 2) {
            // ENOENT
            res.status(404).send(make_error_object(ex));
            utils.log_error("Respond with error code 404 > " + ex);
        } else {
            res.status(500).send(make_error_object(ex));
            utils.log_error("Respond with error code 500 > " + ex);
        }
    } else if(ex instanceof Error) {
        res.status(500).send(make_error_object(ex));
        utils.log_error("Respond with error code 500 > " + ex);
        utils.log_error(ex.stack);
    } else {
        res.status(500).send(make_error_object(ex));
        utils.log_error("Respond with error code 500 > " + ex);
    }
    stat.inc(req, stat.keys.RESPONSE);
    stat.inc(req, stat.keys.RESPONSE_ERROR);
}

module.exports = {
    init: function(param) {
        utils.log_debug("INIT: calling syndicate.create_opts");
        var opts = syndicate.create_opts(param.user, param.volume, param.gateway, param.anonymous, param.debug_level);
        // init UG
        utils.log_debug("INIT: calling syndicate.init");
        return syndicate.init(opts);
    },
    shutdown: function(ug) {
        if(ug) {
            // shutdown UG
            utils.log_debug("SHUTDOWN: calling syndicate.shutdown");
            syndicate.shutdown(ug);
        }
    },
    safeclose: function(ug, fh) {
        if(ug) {
            utils.log_debug("SAFECLOSE: calling syndicate.close");
            syndicate.close(ug, fh);
        }
    },
    getRouter: function() {
        var router = new express.Router();
        router.use(function(req, res, next) {
            utils.log_info(req.method + " " + req.url);
            req.target = querystring.unescape(req.path);
            stat.inc(req, stat.keys.REQUEST);
            next();
        });

        /*
         * HTTP GET: readdir/read/stat/follow operations
         */
        var r_get = function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;
            stat.inc(req, stat.keys.REQUEST_GET);

            if(options.statvfs !== undefined) {
                // statvfs: ?statvfs
                try {
                    utils.log_debug("STATVFS: calling syndicate.statvfs");
                    var ret = syndicate.statvfs(ug);
                    return_data(req, res, ret);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.statvfs_async !== undefined) {
                // statvfs_async: ?statvfs_async
                try {
                    utils.log_debug("STATVFS_ASYNC: calling syndicate.statvfs_async");
                    syndicate.statvfs_async(ug, function(err, statvfs) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, statvfs);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.stat !== undefined) {
                // stat: ?stat
                try {
                    utils.log_debug("STAT: calling syndicate.stat_raw - " + path);
                    var ret = syndicate.stat_raw(ug, path);
                    return_data(req, res, ret);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.stat_async !== undefined) {
                // stat_async: ?stat_async
                try {
                    utils.log_debug("STAT_ASYNC: calling syndicate.stat_raw_async - " + path);
                    syndicate.stat_raw_async(ug, path, function(err, stat) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, stat);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.listdir !== undefined) {
                // listdir: ?listdir
                try {
                    utils.log_debug("LISTDIR: calling syndicate.list_dir - " + path);
                    var entries = syndicate.list_dir(ug, path);
                    var json_obj = {
                        entries: entries
                    };
                    return_data(req, res, json_obj);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.listdir_async !== undefined) {
                // listdir_async: ?listdir_async
                try {
                    utils.log_debug("LISTDIR_ASYNC: calling syndicate.list_dir_async - " + path);
                    syndicate.list_dir_async(ug, path, function(err, entries) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        var json_obj = {
                            entries: entries
                        };

                        return_data(req, res, json_obj);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.getxattr !== undefined) {
                // getxattr: ?getxattr&key='name'
                var key = options.key;
                try {
                    utils.log_debug("GET_XATTR: calling syndicate.get_xattr - " + path + ", " + key);
                    var xattr = syndicate.get_xattr(ug, path, key);
                    var json_obj = {
                        value: xattr
                    };
                    return_data(req, res, json_obj);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.getxattr_async !== undefined) {
                // getxattr_async: ?getxattr_async&key='name'
                var key = options.key;
                try {
                    utils.log_debug("GET_XATTR_ASYNC: calling syndicate.get_xattr_async - " + path + ", " + key);
                    syndicate.get_xattr_async(ug, path, key, function(err, xattr) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        var json_obj = {
                            value: xattr
                        };

                        return_data(req, res, json_obj);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.listxattr !== undefined) {
                // listxattr: ?listxattr
                try {
                    utils.log_debug("LIST_XATTR: calling syndicate.list_xattr - " + path);
                    var xattrs = syndicate.list_xattr(ug, path);
                    var json_obj = {
                        keys: xattrs
                    };
                    return_data(req, res, json_obj);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.listxattr_async !== undefined) {
                // listxattr_async: ?listxattr_async
                try {
                    utils.log_debug("LIST_XATTR_ASYNC: calling syndicate.list_xattr_async - " + path);
                    syndicate.list_xattr_async(ug, path, function(err, xattrs) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        var json_obj = {
                            keys: xattrs
                        };

                        return_data(req, res, json_obj);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.read !== undefined) {
                // read: ?read&fd=fd&offset=offset&len=len
                var offset = Number(options.offset) || 0;
                var len = Number(options.len) || 0;
                try {
                    if(options.fd === undefined) {
                        // stateless
                        utils.log_debug("READ(STATELESS): calling syndicate.open - " + path);
                        var fh = syndicate.open(ug, path, 'r');
                        if(offset !== 0) {
                            utils.log_debug("READ(STATELESS): calling syndicate.seek - " + offset);
                            var new_offset = syndicate.seek(ug, fh, offset);
                            if(new_offset !== offset) {
                                return_data(req, res, new Buffer(0));
                                utils.log_debug("READ(STATELESS): calling syndicate.close");
                                syndicate.close(ug, fh);
                                return;
                            }
                        }

                        utils.log_debug("READ(STATELESS): calling syndicate.read - " + len);
                        var buffer = syndicate.read(ug, fh, len);
                        return_data(req, res, buffer);
                        utils.log_debug("READ(STATELESS): calling syndicate.close");
                        syndicate.close(ug, fh);
                        stat.inc(req, stat.keys.FILE_READ);
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = rfdCache.get(fd);
                        if(fh === undefined) {
                            throw new Error("unable to find a file handle for " + fd);
                        }

                        // extend cache's ttl
                        rfdCache.ttl(fd);
                        utils.log_debug("READ(STATEFUL): calling syndicate.tell");
                        var cur_off = syndicate.tell(ug, fh);
                        if(cur_off !== offset) {
                            utils.log_debug("READ(STATEFUL): calling syndicate.seek - " + offset + ", current " + cur_off);
                            var new_offset = syndicate.seek(ug, fh, offset);
                            if(new_offset !== offset) {
                                return_data(req, res, new Buffer(0));
                                return;
                            }
                        }

                        utils.log_debug("READ(STATEFUL): calling syndicate.read - " + len);
                        var buffer = syndicate.read(ug, fh, len);
                        return_data(req, res, buffer);
                        stat.inc(req, stat.keys.FILE_READ);
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.read_async !== undefined) {
                // read_async: ?read_async&fd=fd&offset=offset&len=len
                var offset = Number(options.offset) || 0;
                var len = Number(options.len) || 0;
                try {
                    if(options.fd === undefined) {
                        // stateless
                        utils.log_debug("READ_ASYNC(STATELESS): calling syndicate.open_async - " + path);
                        syndicate.open_async(ug, path, 'r', function(err, fh) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(offset !== 0) {
                                utils.log_debug("READ_ASYNC(STATELESS): calling syndicate.seek_async - " + offset);
                                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset !== offset) {
                                        return_data(req, res, new Buffer(0));
                                        return;
                                    }

                                    utils.log_debug("READ_ASYNC(STATELESS): calling syndicate.read_async - " + len);
                                    syndicate.read_async(ug, fh, len, function(err, buffer) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

                                        utils.log_debug("READ_ASYNC(STATELESS): calling syndicate.close_async");
                                        syndicate.close_async(ug, fh, function(err, data) {
                                            if(err) {
                                                return_error(req, res, err);
                                                return;
                                            }

                                            return_data(req, res, buffer);
                                            stat.inc(req, stat.keys.FILE_READ);
                                        });
                                    });
                                });
                            } else {
                                utils.log_debug("READ_ASYNC(STATELESS): calling syndicate.read_async");
                                syndicate.read_async(ug, fh, len, function(err, buffer) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    utils.log_debug("READ_ASYNC(STATELESS): calling syndicate.close_async");
                                    syndicate.close_async(ug, fh, function(err, data) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

                                        return_data(req, res, buffer);
                                        stat.inc(req, stat.keys.FILE_READ);
                                    });
                                });
                            }
                        });
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = rfdCache.get(fd);
                        if(fh === undefined) {
                            throw new Error("unable to find a file handle for " + fd);
                        }

                        // extend cache's ttl
                        rfdCache.ttl(fd);

                        utils.log_debug("READ_ASYNC(STATEFUL): calling syndicate.tell");
                        var cur_off = syndicate.tell(ug, fh);
                        if(cur_off !== offset) {
                            utils.log_debug("READ_ASYNC(STATEFUL): calling syndicate.seek_async - " + offset + ", current " + cur_off);
                            syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                if(new_offset !== offset) {
                                    return_data(req, res, new Buffer(0));
                                    return;
                                }

                                utils.log_debug("READ_ASYNC(STATEFUL): calling syndicate.read_async - " + len);
                                syndicate.read_async(ug, fh, len, function(err, buffer) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    return_data(req, res, buffer);
                                    stat.inc(req, stat.keys.FILE_READ);
                                });
                            });
                        } else {
                            utils.log_debug("READ_ASYNC(STATEFUL): calling syndicate.read_async - " + len);
                            syndicate.read_async(ug, fh, len, function(err, buffer) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                return_data(req, res, buffer);
                                stat.inc(req, stat.keys.FILE_READ);
                            });
                        }
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.open !== undefined) {
                // open: ?open&flag='r'
                var flag = options.flag || 'r';
                var newFd = getNextFileDescriptor(req);
                try {
                    utils.log_debug("OPEN: calling syndicate.open - " + path + ", " + flag);
                    var fh = syndicate.open(ug, path, flag);
                    var json_obj = {
                        fd: newFd
                    };
                    return_data(req, res, json_obj);

                    // add to cache
                    if(flag === 'r') {
                        rfdCache.set(newFd, fh);
                    } else {
                        wfdCache.set(newFd, fh);
                    }
                    stat.inc(req, stat.keys.FILE_OPENED);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.open_async !== undefined) {
                // open_async: ?open_async&flag='r'
                var flag = options.flag || 'r';
                var newFd = getNextFileDescriptor(req);
                try {
                    utils.log_debug("OPEN_ASYNC: calling syndicate.open_async - " + path + ", " + flag);
                    syndicate.open_async(ug, path, flag, function(err, fh) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        var json_obj = {
                            fd: newFd
                        };
                        return_data(req, res, json_obj);

                        // add to cache
                        if(flag === 'r') {
                            rfdCache.set(newFd, fh);
                        } else {
                            wfdCache.set(newFd, fh);
                        }
                        stat.inc(req, stat.keys.FILE_OPENED);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.checkopen !== undefined) {
                // checkopen: ?checkopen&fd=fd
                try {
                    if(options.fd === undefined) {
                        throw new Error("fd is not given");
                    }

                    // using the fd
                    // stateful
                    var fd = options.fd;
                    var rfh = rfdCache.get(fd);
                    var wfh = wfdCache.get(fd);

                    var opened = false;
                    if(rfh === undefined && wfh === undefined) {
                        opened = false;
                    } else {
                        opened = true;
                    }

                    var json_obj = {
                        opened: opened
                    };

                    return_data(req, res, json_obj);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else {
                res.status(403).send();
            }
        };

        router.get('*', r_get);

        /*
         * HTTP POST: write/mkdir operations
         */
        var r_post = function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;
            stat.inc(req, stat.keys.REQUEST_POST);

            if(options.mkdir !== undefined) {
                // mkdir: ?mkdir&mode=777
                var mode = options.mode;
                try {
                    utils.log_debug("MKDIR: calling syndicate.mkdir - " + path + ", " + mode);
                    syndicate.mkdir(ug, path, mode);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.mkdir_async !== undefined) {
                // mkdir_async: ?mkdir_async&mode=777
                var mode = options.mode;
                try {
                    utils.log_debug("MKDIR_ASYNC: calling syndicate.mkdir_async - " + path + ", " + mode);
                    syndicate.mkdir_async(ug, path, mode, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.setxattr !== undefined) {
                // setxattr: ?setxattr&key='name'&value='value'
                var key = options.key;
                var value = options.value;
                try {
                    utils.log_debug("SETXATTR: calling syndicate.set_xattr - " + path + ", " + key);
                    syndicate.set_xattr(ug, path, key, val);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.setxattr_async !== undefined) {
                // setxattr_async: ?setxattr_async&key='name'&value='value'
                var key = options.key;
                var value = options.value;
                try {
                    utils.log_debug("SETXATTR_ASYNC: calling syndicate.set_xattr_async - " + path + ", " + key);
                    syndicate.set_xattr_async(ug, path, key, val, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.write !== undefined) {
                // write: ?write&fd=fd&offset=offset&len=len
                var offset = Number(options.offset) || 0;
                var len = Number(options.len) || 0;
                try {
                    if(options.fd === undefined) {
                        // stateless
                        var buffer = new Buffer(len);
                        var buffer_offset = 0;
                        req.on('data', function(chunk) {
                            chunk.copy(buffer, buffer_offset);
                            buffer_offset += chunk.length;
                        });

                        req.on('end', function() {
                            try {
                                utils.log_debug("WRITE(STATELESS): calling syndicate.open - " + path);
                                var fh = syndicate.open(ug, path, 'w');

                                if(offset !== 0) {
                                    utils.log_debug("WRITE(STATELESS): calling syndicate.seek - " + offset);
                                    var new_offset = syndicate.seek(ug, fh, offset);
                                    if(new_offset !== offset) {
                                        return_error(req, res, new Error("can't seek to requested offset"));
                                        utils.log_debug("WRITE(STATELESS): calling syndicate.close");
                                        syndicate.close(ug, fh);
                                        return;
                                    }
                                }

                                utils.log_debug("WRITE(STATELESS): calling syndicate.write");
                                syndicate.write(ug, fh, buffer);

                                utils.log_debug("WRITE(STATELESS): calling syndicate.fsync");
                                syndicate.fsync(ug, fh);

                                utils.log_debug("WRITE(STATELESS): calling syndicate.close");
                                syndicate.close(ug, fh);

                                return_data(req, res, null);
                                stat.inc(req, stat.keys.FILE_WRITE);
                            } catch (ex) {
                                return_error(req, res, ex);
                            }
                        });
                    } else {
                        // using the fd
                        var fd = options.fd;

                        // stateful
                        var buffer = new Buffer(len);
                        var buffer_offset = 0;
                        req.on('data', function(chunk) {
                            chunk.copy(buffer, buffer_offset);
                            buffer_offset += chunk.length;
                        });

                        req.on('end', function() {
                            try {
                                var fh = wfdCache.get(fd);
                                if(fh === undefined) {
                                    throw new Error("unable to find a file handle for " + fd);
                                }

                                // extend cache's ttl
                                wfdCache.ttl(fd);

                                utils.log_debug("WRITE(STATEFUL): calling syndicate.tell");
                                var cur_off = syndicate.tell(ug, fh);
                                if(cur_off !== offset) {
                                    utils.log_debug("WRITE(STATEFUL): calling syndicate.seek - " + offset + ", current " + cur_off);
                                    var new_offset = syndicate.seek(ug, fh, offset);
                                    if(new_offset !== offset) {
                                        return_error(req, res, new Error("can't seek to requested offset"));
                                        return;
                                    }
                                }

                                utils.log_debug("WRITE(STATEFUL): calling syndicate.write");
                                syndicate.write(ug, fh, buffer);

                                return_data(req, res, null);
                                stat.inc(req, stat.keys.FILE_WRITE);
                            } catch (ex) {
                                return_error(req, res, ex);
                            }
                        });
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.write_async !== undefined) {
                // write_async: ?write_async&fd=fd&offset=offset&len=len
                var offset = Number(options.offset) || 0;
                var len = Number(options.len) || 0;
                try {
                    if(options.fd === undefined) {
                        // stateless
                        var buffer = new Buffer(len);
                        var buffer_offset = 0;
                        req.on('data', function(chunk) {
                            chunk.copy(buffer, buffer_offset);
                            buffer_offset += chunk.length;
                        });

                        utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.open_async - ", path);
                        syndicate.open_async(ug, path, 'w', function(err, fh) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(offset !== 0) {
                                utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.seek_async - ", offset);
                                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset !== offset) {
                                        return_error(req, res, new Error("can't seek to requested offset"));
                                        return;
                                    }

                                    req.on('end', function() {
                                        utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.write_async");
                                        syndicate.write_async(ug, fh, buffer, function(err, data) {
                                            if(err) {
                                                return_error(req, res, err);
                                                return;
                                            }

                                            utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.fsync_async");
                                            syndicate.fsync_async(ug, fh, function(err, data) {
                                                if(err) {
                                                    return_error(req, res, err);
                                                    return;
                                                }

                                                utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.close_async");
                                                syndicate.close_async(ug, fh, function(err, data) {
                                                    if(err) {
                                                        return_error(req, res, err);
                                                        return;
                                                    }

                                                    return_data(req, res, null);
                                                    stat.inc(req, stat.keys.FILE_WRITE);
                                                    return;
                                                });
                                            });
                                        });
                                    });
                                });
                            } else {
                                req.on('end', function() {
                                    utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.write_async");
                                    syndicate.write_async(ug, fh, buffer, function(err, data) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

                                        utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.fsync_async");
                                        syndicate.fsync_async(ug, fh, function(err, data) {
                                            if(err) {
                                                return_error(req, res, err);
                                                return;
                                            }

                                            utils.log_debug("WRITE_ASYNC(STATELESS): calling syndicate.close_async");
                                            syndicate.close_async(ug, fh, function(err, data) {
                                                if(err) {
                                                    return_error(req, res, err);
                                                    return;
                                                }

                                                return_data(req, res, null);
                                                stat.inc(req, stat.keys.FILE_WRITE);
                                                return;
                                            });
                                        });
                                    });
                                });
                            }
                        });
                    } else {
                        // using the fd
                        var fd = options.fd;
                        // stateful
                        var buffer = new Buffer(len);
                        var buffer_offset = 0;
                        req.on('data', function(chunk) {
                            chunk.copy(buffer, buffer_offset);
                            buffer_offset += chunk.length;
                        });

                        req.on('end', function() {
                            var fh = wfdCache.get(fd);
                            if(fh === undefined) {
                                throw new Error("unable to find a file handle for " + fd);
                            }

                            // extend cache's ttl
                            wfdCache.ttl(fd);

                            utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.tell");
                            var cur_off = syndicate.tell(ug, fh);
                            if(cur_off !== offset) {
                                utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.seek_async - " + offset + ", current " + cur_off);
                                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset !== offset) {
                                        return_error(req, res, new Error("can't seek to requested offset"));
                                        return;
                                    }

                                    utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.write_async");
                                    syndicate.write_async(ug, fh, buffer, function(err, data) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

                                        return_data(req, res, null);
                                        stat.inc(req, stat.keys.FILE_WRITE);
                                        return;
                                    });
                                });
                            } else {
                                utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.write_async");
                                syndicate.write_async(ug, fh, chunk, function(err, data) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    return_data(req, res, null);
                                    stat.inc(req, stat.keys.FILE_WRITE);
                                    return;
                                });
                            }
                        });
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.extendttl !== undefined) {
                // extendttl: ?extendttl&fd=fd
                try {
                    if(options.fd === undefined) {
                        throw new Error("fd is not given");
                    }

                    // using the fd
                    // stateful
                    var fd = options.fd;
                    var rfh = rfdCache.get(fd);
                    var wfh = wfdCache.get(fd);
                    if(rfh === undefined && wfh === undefined) {
                        throw new Error("could not find a file handle");
                    }

                    if(rfh !== undefined) {
                        // extend cache's ttl
                        rfdCache.ttl(fd);
                    }

                    if(wfh !== undefined) {
                        // extend cache's ttl
                        wfdCache.ttl(fd);
                    }

                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.extendttl_async !== undefined) {
                // extendttl_async: ?extendttl_async&fd=fd
                try {
                    if(options.fd === undefined) {
                        throw new Error("fd is not given");
                    }

                    // using the fd
                    // stateful
                    var fd = options.fd;
                    var rfh = rfdCache.get(fd);
                    var wfh = wfdCache.get(fd);
                    if(rfh === undefined && wfh === undefined) {
                        throw new Error("could not find a file handle");
                    }

                    if(rfh !== undefined) {
                        // extend cache's ttl
                        rfdCache.ttl(fd);
                    }

                    if(wfh !== undefined) {
                        // extend cache's ttl
                        wfdCache.ttl(fd);
                    }

                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            /*
            } else if(options.utimes !== undefined) {
                // utimes: ?utimes&time=new_time
                syndicatefs.utimes(req.target, options, function(err, data) {
                    res.send(err || data);
                });
            */
            } else if(options.rename !== undefined) {
                // rename: ?rename&to='to_filename'
                var to_name = options.to;
                try {
                    utils.log_debug("RENAME: calling syndicate.rename - " + path + ", " + to_name);
                    syndicate.rename(ug, path, to_name);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rename_async !== undefined) {
                // rename_async: ?rename_async&to='to_filename'
                var to_name = options.to;
                try {
                    utils.log_debug("RENAME_ASYNC: calling syndicate.rename_async - " + path + ", " + to_name);
                    syndicate.rename_async(ug, path, to_name, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            /*
            } else if(options.truncate !== undefined) {
                // truncate: ?truncate&offset=offset
                syndicatefs.truncate(req.target, options, function(err, data) {
                    res.send(err || data);
                });
            */
            } else {
                res.status(403).send();
            }
        };

        router.post('*', r_post);

        /*
         * HTTP PUT: write operations
         */
        router.put('*', r_post);

        /*
         * HTTP DELETE: unlink operations
         */
        var r_delete = function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;
            stat.inc(req, stat.keys.REQUEST_DELETE);

            if(options.rmdir !== undefined) {
                // rmdir: ?rmdir
                try {
                    utils.log_debug("RMDIR: calling syndicate.rmdir - " + path);
                    syndicate.rmdir(ug, path);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmdir_async !== undefined) {
                // rmdir_async: ?rmdir_async
                try {
                    utils.log_debug("RMDIR_ASYNC: calling syndicate.rmdir_async - " + path);
                    syndicate.rmdir_async(ug, path, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.unlink !== undefined) {
                // unlink: ?unlink
                try {
                    utils.log_debug("UNLINK: calling syndicate.unlink - " + path);
                    syndicate.unlink(ug, path);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.unlink_async !== undefined) {
                // unlink_async: ?unlink_async
                try {
                    utils.log_debug("UNLINK_ASYNC: calling syndicate.unlink_async - " + path);
                    syndicate.unlink_async(ug, path, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmxattr !== undefined) {
                // rmxattr: ?rmxattr&key='name'
                var key = options.key;
                try {
                    utils.log_debug("RMXATTR: calling syndicate.remove_xattr - " + path + ", " + key);
                    syndicate.remove_xattr(ug, path, key);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmxattr_async !== undefined) {
                // rmxattr_async: ?rmxattr_async&key='name'
                var key = options.key;
                try {
                    utils.log_debug("RMXATTR_ASYNC: calling syndicate.remove_xattr_async - " + path + ", " + key);
                    syndicate.remove_xattr_async(ug, path, key, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                    });
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.close !== undefined) {
                // close: ?close&fd=fd
                var fd = options.fd;
                try {
                    var fh;
                    var rfh = rfdCache.get(fd);
                    var wfh = wfdCache.get(fd);
                    if(rfh === undefined && wfh === undefined) {
                        throw new Error("unable to find a file handle for " + fd);
                    }

                    var wmode = false;
                    if(rfh !== undefined) {
                        fh = rfh;
                        rfdCache.del(fd);
                    }
                    if(wfh !== undefined) {
                        fh = wfh;
                        wfdCache.del(fd);
                        wmode = true;
                    }

                    // write mode
                    if(wmode) {
                        utils.log_debug("CLOSE: calling syndicate.fsync");
                        syndicate.fsync(ug, fh);
                    }

                    utils.log_debug("CLOSE: calling syndicate.close");
                    syndicate.close(ug, fh);
                    stat.dec(req, stat.keys.FILE_OPENED);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.close_async !== undefined) {
                // close_async: ?close_async&fd=fd
                var fd = options.fd;
                try {
                    var fh;
                    var rfh = rfdCache.get(fd);
                    var wfh = wfdCache.get(fd);
                    if(rfh === undefined && wfh === undefined) {
                        throw new Error("unable to find a file handle for " + fd);
                    }

                    var wmode = false;
                    if(rfh !== undefined) {
                        fh = rfh;
                        rfdCache.del(fd);
                    }
                    if(wfh !== undefined) {
                        fh = wfh;
                        wfdCache.del(fd);
                        wmode = true;
                    }

                    // write mode
                    if(wmode) {
                        utils.log_debug("CLOSE_ASYNC: calling syndicate.fsync_async");
                        syndicate.fsync_async(ug, fh, function(err, data) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            utils.log_debug("CLOSE_ASYNC: calling syndicate.close_async");
                            syndicate.close_async(ug, fh, function(err, data) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                return_data(req, res, null);
                                stat.dec(req, stat.keys.FILE_OPENED);
                            });
                        });
                    } else {
                        utils.log_debug("CLOSE_ASYNC: calling syndicate.close_async");
                        syndicate.close_async(ug, fh, function(err, data) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            return_data(req, res, null);
                            stat.dec(req, stat.keys.FILE_OPENED);
                        });
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else {
                res.status(403).send();
            }
        };

        router.delete('*', r_delete);
        return router;
    }
};
