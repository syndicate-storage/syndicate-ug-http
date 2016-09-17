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
        console.log('stat_inc %s - %d', key, statistics[key]);
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

        console.log('stat_dec %s - %d', key, statistics[key]);
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
        console.error("Respond with data (code 200) > " + data.length + " bytes");
    } else {
        console.error("Respond with data (code 200)");
    }
}

function return_error(req, res, ex) {
    if(ex instanceof syndicate.syndicate_error) {
        if(ex.extra === 2) {
            // ENOENT
            res.status(404).send(make_error_object(ex));
            console.error("Respond with error code 404 > " + ex);
        } else {
            res.status(500).send(make_error_object(ex));
            console.error("Respond with error code 500 > " + ex);
        }
    } else if(ex instanceof Error) {
        res.status(500).send(make_error_object(ex));
        console.error("Respond with error code 500 > " + ex);
        console.error(ex.stack);
    } else {
        res.status(500).send(make_error_object(ex));
        console.error("Respond with error code 500 > " + ex);
    }
    stat.inc(req, stat.keys.RESPONSE);
    stat.inc(req, stat.keys.RESPONSE_ERROR);
}

module.exports = {
    init: function(param) {
        var opts = syndicate.create_opts(param.user, param.volume, param.gateway, param.anonymous, param.debug_level);
        // init UG
        return syndicate.init(opts);
    },
    shutdown: function(ug) {
        if(ug) {
            // shutdown UG
            syndicate.shutdown(ug);
        }
    },
    safeclose: function(ug, fh) {
        if(ug) {
            syndicate.close(ug, fh);
        }
    },
    getRouter: function() {
        var router = new express.Router();
        router.use(function(req, res, next) {
            console.log('%s %s', req.method, req.url);
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
                    var ret = syndicate.statvfs(ug);
                    return_data(req, res, ret);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.statvfs_async !== undefined) {
                // statvfs_async: ?statvfs_async
                try {
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
                    var ret = syndicate.stat_raw(ug, path);
                    return_data(req, res, ret);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.stat_async !== undefined) {
                // stat_async: ?stat_async
                try {
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
                        var fh = syndicate.open(ug, path, 'r');
                        if(offset !== 0) {
                            var new_offset = syndicate.seek(ug, fh, offset);
                            if(new_offset != offset) {
                                return_data(req, res, new Buffer(0));
                                syndicate.close(ug, fh);
                                return;
                            }
                        }

                        var buffer = syndicate.read(ug, fh, len);
                        return_data(req, res, buffer);
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

                        var new_offset = syndicate.seek(ug, fh, offset);
                        if(new_offset != offset) {
                            return_data(req, res, new Buffer(0));
                            return;
                        }

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
                        syndicate.open_async(ug, path, 'r', function(err, fh) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(offset !== 0) {
                                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset != offset) {
                                        return_data(req, res, new Buffer(0));
                                        return;
                                    }

                                    syndicate.read_async(ug, fh, len, function(err, buffer) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

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
                                syndicate.read_async(ug, fh, len, function(err, buffer) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    syndicate.close_async(ug, fh, function(err, data) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

                                        return_data(req, res, buffer);
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

                        syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(new_offset != offset) {
                                return_data(req, res, new Buffer(0));
                                return;
                            }

                            syndicate.read_async(ug, fh, len, function(err, buffer) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                return_data(req, res, buffer);
                                stat.inc(req, stat.keys.FILE_READ);
                            });
                        });
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.open !== undefined) {
                // open: ?open&flag='r'
                var flag = options.flag || 'r';
                var newFd = getNextFileDescriptor(req);
                try {
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
                    syndicate.mkdir(ug, path, mode);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.mkdir_async !== undefined) {
                // mkdir_async: ?mkdir_async&mode=777
                var mode = options.mode;
                try {
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
                        var fh = syndicate.open(ug, path, 'w');
                        if(offset !== 0) {
                            var new_offset = syndicate.seek(ug, fh, offset);
                            if(new_offset != offset) {
                                return_data(req, res, null);
                                syndicate.close(ug, fh);
                                return;
                            }
                        }

                        req.on('data', function(chunk) {
                            syndicate.write(ug, fh, chunk);
                        });

                        req.on('end', function() {
                            return_data(req, res, null);
                            syndicate.close(ug, fh);
                            stat.inc(req, stat.keys.FILE_WRITE);
                        });
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = wfdCache.get(fd);
                        if(fh === undefined) {
                            throw new Error("unable to find a file handle for " + fd);
                        }

                        // extend cache's ttl
                        wfdCache.ttl(fd);

                        var new_offset = syndicate.seek(ug, fh, offset);
                        if(new_offset != offset) {
                            return_data(req, res, new Buffer(0));
                            return;
                        }

                        req.on('data', function(chunk) {
                            syndicate.write(ug, fh, chunk);
                        });

                        req.on('end', function() {
                            return_data(req, res, null);
                            stat.inc(req, stat.keys.FILE_WRITE);
                            return;
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
                        syndicate.open_async(ug, path, 'w', function(err, fh) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(offset !== 0) {
                                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset != offset) {
                                        return_data(req, res, new Buffer(0));
                                        return;
                                    }

                                    write_err = null;

                                    req.on('data', function(chunk) {
                                        if(write_err) {
                                            return;
                                        }

                                        syndicate.write_async(ug, fh, chunk, function(err, data) {
                                            if(err) {
                                                write_err = err;
                                                return;
                                            }
                                        });
                                    });

                                    req.on('end', function() {
                                        if(write_err) {
                                            return_error(req, res, write_err);
                                            return;
                                        } else {
                                            syndicate.close_async(ug, fh, function(err, data) {
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
                                });
                            } else {
                                write_err = null;

                                req.on('data', function(chunk) {
                                    if(write_err) {
                                        return;
                                    }

                                    syndicate.write_async(ug, fh, chunk, function(err, data) {
                                        if(err) {
                                            write_err = err;
                                            return;
                                        }
                                    });
                                });

                                req.on('end', function() {
                                    if(write_err) {
                                        return_error(req, res, write_err);
                                        return;
                                    } else {
                                        syndicate.close_async(ug, fh, function(err, data) {
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
                        });
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = wfdCache.get(fd);
                        if(fh === undefined) {
                            throw new Error("unable to find a file handle for " + fd);
                        }

                        // extend cache's ttl
                        wfdCache.ttl(fd);

                        syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(new_offset != offset) {
                                return_data(req, res, new Buffer(0));
                                return;
                            }

                            write_err = null;

                            req.on('data', function(chunk) {
                                if(write_err) {
                                    return;
                                }

                                syndicate.write_async(ug, fh, chunk, function(err, data) {
                                    if(err) {
                                        write_err = err;
                                        return;
                                    }
                                });
                            });

                            req.on('end', function() {
                                if(write_err) {
                                    return_error(req, res, write_err);
                                    return;
                                } else {
                                    return_data(req, res, null);
                                    stat.inc(req, stat.keys.FILE_WRITE);
                                    return;
                                }
                            });
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
                    syndicate.rename(ug, path, to_name);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rename_async !== undefined) {
                // rename_async: ?rename_async&to='to_filename'
                var to_name = options.to;
                try {
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
                    syndicate.rmdir(ug, path);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmdir_async !== undefined) {
                // rmdir_async: ?rmdir_async
                try {
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
                    syndicate.unlink(ug, path);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.unlink_async !== undefined) {
                // unlink_async: ?unlink_async
                try {
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
                    syndicate.remove_xattr(ug, path, key);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmxattr_async !== undefined) {
                // rmxattr_async: ?rmxattr_async&key='name'
                var key = options.key;
                try {
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

                    if(rfh !== undefined) {
                        fh = rfh;
                        rfdCache.del(fd);
                    }
                    if(wfh !== undefined) {
                        fh = wfh;
                        wfdCache.del(fd);
                    }

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

                    if(rfh !== undefined) {
                        fh = rfh;
                        rfdCache.del(fd);
                    }
                    if(wfh !== undefined) {
                        fh = wfh;
                        wfdCache.del(fd);
                    }

                    syndicate.close_async(ug, fh, function(err, data) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        return_data(req, res, null);
                        stat.dec(req, stat.keys.FILE_OPENED);
                    });
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
