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

function make_error_object(ex) {
    return {
        name: ex.name,
        message: ex.message,
    };
}

module.exports = {
    init: function(param) {
        var opts = syndicate.create_opts(param.user, param.volume, param.gateway, param.anonymous);
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
            req.target = querystring.unescape(req.path);
            next();
        });

        /*
         * HTTP GET: readdir/read/stat/follow operations
         */
        router.get('*', function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;

            if(options.stat !== undefined) {
                // stat: ?stat
                try {
                    var ret = syndicate.stat_raw(ug, path);
                    res.status(200).send(ret);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.listdir !== undefined) {
                // stat: ?listdir
                try {
                    var ret = syndicate.list_dir(ug, path);
                    res.status(200).send(ret);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.getxattr !== undefined) {
                // getxattr: ?getxattr&key='name'
                var key = options.key;
                try {
                    var ret = syndicate.get_xattr(ug, path, key);
                    var json_obj = {
                        value: ret
                    };
                    res.status(200).send(json_obj);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.listxattr !== undefined) {
                // listxattr: ?listxattr
                try {
                    var ret = syndicate.list_xattr(ug, path);
                    res.status(200).send(ret);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
                                res.status(200).send(new Buffer(0));
                            }
                        }

                        var buffer = syndicate.read(ug, fh, len);
                        res.status(200).send(buffer);
                        syndicate.close(ug, fh);
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = rfdCache.get(fd);
                        if( fh === undefined ) {
                            throw "unable to find a file handle for " + fd;
                        }

                        var new_offset = syndicate.seek(ug, fh, offset);
                        if(new_offset != offset) {
                            res.status(200).send(new Buffer(0));
                        }

                        var buffer = syndicate.read(ug, fh, len);
                        res.status(200).send(buffer);

                        // extend cache's ttl
                        rfdCache.ttl(fd);
                    }
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.open !== undefined) {
                // open: ?open&flag='r'
                var flag = options.flag || 'r';
                var newFd = g_fd++;
                try {
                    var fh = syndicate.open(ug, path, flag);
                    var json_obj = {
                        fd: newFd
                    };
                    res.status(200).send(json_obj);

                    // add to cache
                    if( flag === 'r' ) {
                        rfdCache.set(newFd, fh);
                    } else {
                        wfdCache.set(newFd, fh);
                    }
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else {
                res.status(403).send();
            }
        });

        /*
         * HTTP POST: write/mkdir operations
         */
        router.post('*', function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;

            if(options.mkdir !== undefined) {
                // mkdir: ?mkdir&mode=777
                var mode = req.mode;
                try {
                    syndicate.mkdir(ug, path, mode);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.setxattr !== undefined) {
                // setxattr: ?setxattr&key='name'&value='value'
                var key = req.key;
                var value = req.value;
                try {
                    syndicate.set_xattr(ug, path, key, val);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
                                res.status(200).send();
                            }
                        }

                        stream.on('data', function(chunk) {
                            syndicate.write(ug, fh, chunk);
                        });

                        res.status(200).send();
                        syndicate.close(ug, fh);
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = wfdCache.get(fd);
                        if( fh === undefined ) {
                            throw "unable to find a file handle for " + fd;
                        }

                        var new_offset = syndicate.seek(ug, fh, offset);
                        if(new_offset != offset) {
                            res.status(200).send(new Buffer(0));
                        }

                        stream.on('data', function(chunk) {
                            syndicate.write(ug, fh, chunk);
                        });
                        res.status(200).send();

                        // extend cache's ttl
                        wfdCache.ttl(fd);
                    }
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
                var to_name = req.to;
                try {
                    syndicate.rename(ug, path, to_name);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
        });

        /*
         * HTTP PUT: write operations
         */
        router.put('*', function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;

            if(options.mkdir !== undefined) {
                // mkdir: ?mkdir&mode=777
                var mode = req.mode;
                try {
                    syndicate.mkdir(ug, path, mode);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.setxattr !== undefined) {
                // setxattr: ?setxattr&key='name'&value='value'
                var key = req.key;
                var value = req.value;
                try {
                    syndicate.set_xattr(ug, path, key, val);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
                                res.status(200).send();
                            }
                        }

                        stream.on('data', function(chunk) {
                            syndicate.write(ug, fh, chunk);
                        });

                        res.status(200).send();
                        syndicate.close(ug, fh);
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var fh = wfdCache.get(fd);
                        if( fh === undefined ) {
                            throw "unable to find a file handle for " + fd;
                        }

                        var new_offset = syndicate.seek(ug, fh, offset);
                        if(new_offset != offset) {
                            res.status(200).send(new Buffer(0));
                        }

                        stream.on('data', function(chunk) {
                            syndicate.write(ug, fh, chunk);
                        });
                        res.status(200).send();

                        // extend cache's ttl
                        wfdCache.ttl(fd);
                    }
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
                var to_name = req.to;
                try {
                    syndicate.rename(ug, path, to_name);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
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
        });

        /*
         * HTTP DELETE: unlink operations
         */
        router.delete('*', function(req, res) {
            var options = req.query;
            var path = req.target;
            var ug = req.ug;
            var rfdCache = req.rfdCache;
            var wfdCache = req.wfdCache;

            if(options.rmdir !== undefined) {
                // rmdir: ?rmdir
                try {
                    syndicate.rmdir(ug, path);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.unlink !== undefined) {
                // unlink: ?unlink
                try {
                    syndicate.unlink(ug, path);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.rmxattr !== undefined) {
                // rmxattr: ?rmxattr&key='name'
                var key = req.key;
                try {
                    syndicate.remove_xattr(ug, path, key);
                    res.status(200).send();
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));
                }
            } else if(options.close !== undefined) {
                // close: ?close&fd=fd
                var fd = options.fd;
                try {
                    var fh = -1;
                    var rfh = rfdCache.get(fd);
                    var wfh = wfdCache.get(fd);
                    if( rfh === undefined && wfh === undefined ) {
                        throw "unable to find a file handle for " + fd;
                    }

                    if( rfh !== undefined ) {
                        fh = rfh;
                    }
                    if( wfh !== undefined ) {
                        fh = wfh;
                    }

                    syndicate.close(ug, fh);
                    res.status(200).send();

                    // delete from cache
                    if( rfh !== undefined ) {
                        rfdCache.del(fd);
                    }
                    if( wfh !== undefined ) {
                        wfdCache.del(fd);
                    }
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(make_error_object(ex));             
                }
            } else {
                res.status(403).send();
            }
        });
        return router;
    }
};
