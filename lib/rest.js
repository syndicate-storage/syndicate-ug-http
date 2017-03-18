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

var express = require('express');
var expressSession = require('express-session');
var querystring = require('querystring');
var fs = require('fs');
var multer  = require('multer');
var async = require('async');
var syndicate = require('syndicate-drive');
var utils = require('./utils.js');
var sessions = require('./sessions.js');
var gatewayState = require('./gateway_state.js');
var syndicateSetup = require('./syndicate_setup.js');
var passport = require('passport');
var basicStrategy = require('passport-http').BasicStrategy;


var UPLOADS_PATH = '/tmp/syndicate-ug-http/uploads';

var storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, UPLOADS_PATH);
    },
    filename: function (req, file, cb) {
        cb(null, util.format("%s.bin", Date.now()));
    }
});
var upload = multer({ storage: storage });

var authenticate = passport.authenticate('basic');

function make_error_object(ex) {
    if(ex instanceof Error) {
        return {
            result: false,
            name: ex.name,
            message: ex.message,
        };
    } else {
        return {
            result: false,
            name: "error",
            message: ex,
        };
    }
}

function return_data(req, res, data) {
    // return with HTTP 200 code
    res.status(200).send(data);
    
    if(data instanceof Buffer) {
        utils.log_info(util.format("Respond with data (code 200) > %d bytes", data.length));
    } else {
        utils.log_info("Respond with data (code 200)");
    }
}

function return_boolean(req, res, success) {
    var ret_data = {
        result: success
    };
    return_data(req, res, ret_data);
}

function return_error_raw(req, res, error_code, error) {
    // return with HTTP error code
    error_code = error_code || 404;
    res.status(error_code).send(error);
    
    if(error instanceof Buffer) {
        utils.log_info(util.format("Respond with error (code %d) > %d bytes", error_code, error.length));
    } else {
        utils.log_info(util.format("Respond with error (code %d)", error_code));
    }
}

function return_forbidden(req, res, msg) {
    return_error_raw(req, res, 403, make_error_object(msg));
}

function return_badrequest(req, res, msg) {
    return_error_raw(req, res, 400, make_error_object(msg));
}

function return_error(req, res, ex) {
    if(ex instanceof syndicate.syndicate_error) {
        if(ex.extra === 2) {
            // ENOENT
            return_error_raw(req, res, 404, make_error_object(ex));
        } else {
            return_error_raw(req, res, 500, make_error_object(ex));
        }
    } else if(ex instanceof Error) {
        return_error_raw(req, res, 500, make_error_object(ex));
        utils.log_error(ex.stack);
    } else {
        return_error_raw(req, res, 500, make_error_object(ex));
    }
}

function get_field(field, options, body) {
    var field_val = null;
    if(field in options) {
        field_val = options[field];
    }

    if(field in req.body) {
        field_val = body[ms_url];
    }

    if(field_val !== null && field_val.length !== 0) {
        return field_val;
    }

    return null;
}

module.exports = {
    init: function(app, param) {
        // init REST
        utils.log_debug("INIT: initializing REST framework");

        // set session
        app.use(expressSession({
            secret: util.generate_random_string(64),
            resave: false,
        }));

        // create session-manager
        var sess = sessions.init();
        app.use(function(req, res, next) {
            req.sessions = sess;
            next();
        });

        // authentication
        passport.use(new basicStrategy(function(username, password, callback) {
            sessions.authenticate_async(sess, username, password, callback);
        }));
        passport.serializeUser(function(session, callback) {
            callback(null, sessions.serialize_session(session));
        });
        passport.deserializeUser(function(session_name, callback) {
            callback(null, sessions.deserialize_session(session_name));
        });

        app.use(passport.initialize());
        app.use(passport.session());

        utils.create_dir_recursively_sync(UPLOADS_PATH);
    },
    get_router: function() {
        var router = new express.Router();
        router.use(function(req, res, next) {
            utils.log_info(util.format("%s %s", req.method, req.url));
            req.target = querystring.unescape(req.path);
            next();
        });

        // user setup
        router.post('/setup/user', upload.single('cert'), function(req, res) {
            var options = req.query;
            var sess = req.sessions;
            
            var ms_url = get_field("ms_url", options, req.body);
            if(ms_url === null) {
                return_badrequest(req, res, "invalid request parameters - ms_url is not given");
                return;
            }
            
            var user = get_field("user", options, req.body);
            if(user === null) {
                return_badrequest(req, res, "invalid request parameters - user is not given");
                return;
            }
            
            var cert_file = req.file;
            if(cert_file === null || cert_file === undefined) {
                return_badrequest(req, res, "invalid request parameters - cert is not given");
                return;
            }
            
            var cert_path = cert_file.path;
            
            utils.log_debug(util.format("setting up a user - %s for %s", user, ms_url));
            async.waterfall([
                function(cb) {
                    // register user (import)
                    syndicateSetup.setup_user(ms_url, user, cert_path, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                },
                function(result, cb) {
                    // remove cert after installing it
                    fs.unlink(cert_path, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                }
            ], function(err, data) {
                if(err) {
                    return_error(req, res, err);
                    return;
                }
                
                return_boolean(req, res, true);
            });
        });
        
        // gateway setup
        router.post('/setup/gateway', upload.single('cert'), function(req, res) {
            var options = req.query;
            var sess = req.sessions;

            var session_name = get_field("session_name", options, req.body);
            if(session_name === null) {
                return_badrequest(req, res, "invalid request parameters - session_name is not given");
                return;
            }

            var session_key = get_field("session_key", options, req.body);
            if(session_key === null) {
                return_badrequest(req, res, "invalid request parameters - session_key is not given");
                return;
            }

            var ms_url = get_field("ms_url", options, req.body);
            if(ms_url === null) {
                return_badrequest(req, res, "invalid request parameters - ms_url is not given");
                return;
            }

            var user = get_field("user", options, req.body);
            if(user === null) {
                return_badrequest(req, res, "invalid request parameters - user is not given");
                return;
            }

            var volume = get_field("volume", options, req.body);
            if(volume === null) {
                return_badrequest(req, res, "invalid request parameters - volume is not given");
                return;
            }

            var gateway = get_field("gateway", options, req.body);
            if(gateway === null) {
                return_badrequest(req, res, "invalid request parameters - gateway is not given");
                return;
            }

            var anonymous_str = get_field("anonymous", options, req.body);
            var anonymous = false;
            if(anonymous_str.trim() === "true") {
                anonymous = true;
            }

            var cert_path = null;
            if(!anonymous) {
                var cert_file = req.file;
                if(cert_file === null || cert_file === undefined) {
                    return_badrequest(req, res, "invalid request parameters - cert is not given");
                    return;
                }
                
                cert_path = cert_file.path;
            }
            
            utils.log_debug(util.format("setting up a gateway - U(%s) / V(%s) / G(%s) for MS(%s) with a session (%s)", user, volume, gateway, ms_url, session_name));
            async.waterfall([
                function(cb) {
                    // register gateway (import)
                    syndicateSetup.setup_gateway(ms_url, user, volume, gateway, anonymous, cert_path, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                },
                function(result, cb) {
                    // remove cert after installing it
                    if(cert_path) {
                        fs.unlink(cert_path, function(err, data) {
                            if(err) {
                                cb(err, null);
                                return;
                            }
                            cb(null, data);
                        });
                    } else {
                        cb(null, true);
                    }
                },
                function(result, cb) {
                    // register session
                    try {
                        sessions.create_session(sess, session_name, session_key, ms_url, user, volume, gateway, anonymous, get_configuration_path());
                        cb(null, true);
                    } catch (e) {
                        cb(e, null);
                    }
                }
            ], function(err, data) {
                if(err) {
                    return_error(req, res, err);
                    return;
                }
                
                return_boolean(req, res, true);
            });
        });

        /*
         * From this point, authentication is required.
         */

        /*
         * HTTP GET: readdir/read/stat/follow operations
         */
        var r_get = function(req, res) {
            var session_name = req.user;
            var options = req.query;
            var path = req.target;
            var session = sessions.get_session(req.sessions, session_name);
            if(session === null) {
                // session not exist
                return_forbidden(req, res, util.format("cannot retrieve a session from a key - %s", session_key));
                return;
            }

            var gateway_state = session.gateway_state;
            if(gateway_state === null) {
                // session not exist
                return_forbidden(req, res, util.format("cannot retrieve a gateawy from a key - %s", session_key));
                return;
            }
            
            var ug = gateway_state.ug;
            
            //gatewayState.inc_statistics(gstate, gatewayState.statistics_keys.REQUEST_GET);
            
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
                    utils.log_debug(util.format("STAT: calling syndicate.stat_raw - %s", path));
                    var ret = syndicate.stat_raw(ug, path);
                    return_data(req, res, ret);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.stat_async !== undefined) {
                // stat_async: ?stat_async
                try {
                    utils.log_debug(util.format("STAT_ASYNC: calling syndicate.stat_raw_async - %s", path));
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
                    utils.log_debug(util.format("LISTDIR: calling syndicate.list_dir - %s", path));
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
                    utils.log_debug(util.format("LISTDIR_ASYNC: calling syndicate.list_dir_async - %s", path));
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
                    utils.log_debug(util.format("GET_XATTR: calling syndicate.get_xattr - %s, %s", path, key));
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
                    utils.log_debug(util.format("GET_XATTR_ASYNC: calling syndicate.get_xattr_async - %s, %s", path, key));
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
                    utils.log_debug(util.format("LIST_XATTR: calling syndicate.list_xattr - %s", path));
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
                    utils.log_debug(util.format("LIST_XATTR_ASYNC: calling syndicate.list_xattr_async - %s", path));
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
                        utils.log_debug(util.format("READ(STATELESS): calling syndicate.open - %s", path));
                        var fh = syndicate.open(ug, path, 'r');
                        if(offset !== 0) {
                            utils.log_debug(util.format("READ(STATELESS): calling syndicate.seek - %d", offset));
                            var new_offset = syndicate.seek(ug, fh, offset);
                            if(new_offset !== offset) {
                                return_data(req, res, new Buffer(0));
                                utils.log_debug("READ(STATELESS): calling syndicate.close");
                                syndicate.close(ug, fh);
                                return;
                            }
                        }

                        utils.log_debug(util.format("READ(STATELESS): calling syndicate.read - %d", len));
                        var buffer = syndicate.read(ug, fh, len);
                        return_data(req, res, buffer);
                        utils.log_debug("READ(STATELESS): calling syndicate.close");
                        syndicate.close(ug, fh);
                        //stat.inc(req, stat.keys.FILE_READ);
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var stat = gatewayState.stat_file_handle(gateway_state, fd);
                        if(stat === null) {
                            throw new Error(util.format("unable to find a file handle for %d", fd));
                        }

                        if(stat.flag !== 'r') {
                            throw new Error(util.format("file handle %d is not for read", fd));
                        }

                        utils.log_debug("READ(STATEFUL): calling syndicate.tell");
                        var cur_off = syndicate.tell(ug, stat.fh);
                        if(cur_off !== offset) {
                            utils.log_debug(util.format("READ(STATEFUL): calling syndicate.seek - %d, current %d", offset, cur_off));
                            var new_offset = syndicate.seek(ug, stat.fh, offset);
                            if(new_offset !== offset) {
                                return_data(req, res, new Buffer(0));
                                return;
                            }
                        }

                        utils.log_debug(util.format("READ(STATEFUL): calling syndicate.read - %d", len));
                        var buffer = syndicate.read(ug, stat.fh, len);
                        return_data(req, res, buffer);
                        //stat.inc(req, stat.keys.FILE_READ);
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
                        utils.log_debug(util.format("READ_ASYNC(STATELESS): calling syndicate.open_async - %s", path));
                        syndicate.open_async(ug, path, 'r', function(err, fh) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(offset !== 0) {
                                utils.log_debug(util.format("READ_ASYNC(STATELESS): calling syndicate.seek_async - %d", offset));
                                syndicate.seek_async(ug, fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset !== offset) {
                                        return_data(req, res, new Buffer(0));
                                        return;
                                    }

                                    utils.log_debug(util.format("READ_ASYNC(STATELESS): calling syndicate.read_async - %d", len));
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
                                            //stat.inc(req, stat.keys.FILE_READ);
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
                                        //stat.inc(req, stat.keys.FILE_READ);
                                    });
                                });
                            }
                        });
                    } else {
                        // using the fd
                        // stateful
                        var fd = options.fd;
                        var stat = gatewayState.stat_read_file_handle(gateway_state, fd);
                        if(stat === null) {
                            throw new Error(util.format("unable to find a file handle for %d", fd));
                        }

                        if(stat.flag !== 'r') {
                            throw new Error(util.format("file handle %d is not for read", fd));
                        }

                        utils.log_debug("READ_ASYNC(STATEFUL): calling syndicate.tell");
                        var cur_off = syndicate.tell(ug, stat.fh);
                        if(cur_off !== offset) {
                            utils.log_debug(util.format("READ_ASYNC(STATEFUL): calling syndicate.seek_async - %d, current %d", offset, cur_off));
                            syndicate.seek_async(ug, stat.fh, offset, function(err, new_offset) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                if(new_offset !== offset) {
                                    return_data(req, res, new Buffer(0));
                                    return;
                                }

                                utils.log_debug(util.format("READ_ASYNC(STATEFUL): calling syndicate.read_async - %d", len));
                                syndicate.read_async(ug, stat.fh, len, function(err, buffer) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    return_data(req, res, buffer);
                                    //stat.inc(req, stat.keys.FILE_READ);
                                });
                            });
                        } else {
                            utils.log_debug(util.format("READ_ASYNC(STATEFUL): calling syndicate.read_async - %d", len));
                            syndicate.read_async(ug, stat.fh, len, function(err, buffer) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                return_data(req, res, buffer);
                                //stat.inc(req, stat.keys.FILE_READ);
                            });
                        }
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.open !== undefined) {
                // open: ?open&flag='r'
                var flag = options.flag || 'r';
                try {
                    utils.log_debug(util.format("OPEN: calling syndicate.open - %s, %s", path, flag));
                    var fh = syndicate.open(ug, path, flag);
                    var newFd = gatewayState.create_file_handle(gateway_state, path, fh, flag);

                    var json_obj = {
                        fd: newFd
                    };
                    return_data(req, res, json_obj);

                    //stat.inc(req, stat.keys.FILE_OPENED);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.open_async !== undefined) {
                // open_async: ?open_async&flag='r'
                var flag = options.flag || 'r';
                try {
                    utils.log_debug(util.format("OPEN_ASYNC: calling syndicate.open_async - %s, %s", path, flag));
                    syndicate.open_async(ug, path, flag, function(err, fh) {
                        if(err) {
                            return_error(req, res, err);
                            return;
                        }

                        var newFd = gatewayState.create_file_handle(gateway_state, path, fh, flag);
                        var json_obj = {
                            fd: newFd
                        };
                        return_data(req, res, json_obj);

                        //stat.inc(req, stat.keys.FILE_OPENED);
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
                    var stat = gatewayState.stat_file_handle(gateway_state, fd);
                    
                    var json_obj = {
                        'opened': stat === null ? false : true
                    };
                    
                    return_data(req, res, json_obj);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else {
                res.status(403).send();
            }
        };
        router.get('*', authenticate, r_get);
        
        /*
         * HTTP POST: write/mkdir operations
         */
        var r_post = function(req, res) {
            var session_name = req.user;
            var options = req.query;
            var path = req.target;
            var session = sessions.get_session(req.sessions, session_name);
            if(session === null) {
                // session not exist
                return_forbidden(req, res, util.format("cannot retrieve a session from a key - %s", session_key));
                return;
            }

            var gateway_state = session.gateway_state;
            if(gateway_state === null) {
                // session not exist
                return_forbidden(req, res, util.format("cannot retrieve a gateawy from a key - %s", session_key));
                return;
            }
            
            var ug = gateway_state.ug;

            //stat.inc(req, stat.keys.REQUEST_POST);

            if(options.mkdir !== undefined) {
                // mkdir: ?mkdir&mode=777
                var mode = options.mode;
                try {
                    utils.log_debug(util.format("MKDIR: calling syndicate.mkdir - %s, %s", path, mode));
                    syndicate.mkdir(ug, path, mode);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.mkdir_async !== undefined) {
                // mkdir_async: ?mkdir_async&mode=777
                var mode = options.mode;
                try {
                    utils.log_debug(util.format("MKDIR_ASYNC: calling syndicate.mkdir_async - %s, %s", path, mode));
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
                    utils.log_debug(util.format("SETXATTR: calling syndicate.set_xattr - %s, %s", path, key));
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
                    utils.log_debug(util.format("SETXATTR_ASYNC: calling syndicate.set_xattr_async - %s, %s", path, key));
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
                                utils.log_debug(util.format("WRITE(STATELESS): calling syndicate.open - %s", path));
                                var fh = syndicate.open(ug, path, 'w');

                                if(offset !== 0) {
                                    utils.log_debug(util.format("WRITE(STATELESS): calling syndicate.seek - %d", offset));
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
                                //stat.inc(req, stat.keys.FILE_WRITE);
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
                                var stat = gatewayState.stat_write_file_handle(gateway_state, fd);
                                if(stat === null) {
                                    return_error(req, res, new Error(util.format("unable to find a file handle for %d", fd)));
                                    return;
                                }

                                if(stat.flag !== 'w') {
                                    throw new Error(util.format("file handle %d is not for write", fd));
                                }

                                utils.log_debug("WRITE(STATEFUL): calling syndicate.tell");
                                var cur_off = syndicate.tell(ug, stat.fh);
                                if(cur_off !== offset) {
                                    utils.log_debug(util.format("WRITE(STATEFUL): calling syndicate.seek - %d, current %d", offset, cur_off));
                                    var new_offset = syndicate.seek(ug, stat.fh, offset);
                                    if(new_offset !== offset) {
                                        return_error(req, res, new Error("can't seek to requested offset"));
                                        return;
                                    }
                                }

                                utils.log_debug("WRITE(STATEFUL): calling syndicate.write");
                                syndicate.write(ug, stat.fh, buffer);

                                return_data(req, res, null);
                                //stat.inc(req, stat.keys.FILE_WRITE);
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

                        utils.log_debug(util.format("WRITE_ASYNC(STATELESS): calling syndicate.open_async - %s", path));
                        syndicate.open_async(ug, path, 'w', function(err, fh) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            if(offset !== 0) {
                                utils.log_debug(util.format("WRITE_ASYNC(STATELESS): calling syndicate.seek_async - %d", offset));
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
                                                    //stat.inc(req, stat.keys.FILE_WRITE);
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
                                                //stat.inc(req, stat.keys.FILE_WRITE);
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
                            var stat = gatewayState.stat_write_file_handle(gateway_state, fd);
                            if(stat === null) {
                                return_error(req, res, new Error(util.format("unable to find a file handle for %d", fd)));
                                return;
                            }

                            if(stat.flag !== 'w') {
                                throw new Error(util.format("file handle %d is not for write", fd));
                            }

                            utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.tell");
                            var cur_off = syndicate.tell(ug, stat.fh);
                            if(cur_off !== offset) {
                                utils.log_debug(util.format("WRITE_ASYNC(STATEFUL): calling syndicate.seek_async - %d, current %d", offset, cur_off));
                                syndicate.seek_async(ug, stat.fh, offset, function(err, new_offset) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    if(new_offset !== offset) {
                                        return_error(req, res, new Error("can't seek to requested offset"));
                                        return;
                                    }

                                    utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.write_async");
                                    syndicate.write_async(ug, stat.fh, buffer, function(err, data) {
                                        if(err) {
                                            return_error(req, res, err);
                                            return;
                                        }

                                        return_data(req, res, null);
                                        //stat.inc(req, stat.keys.FILE_WRITE);
                                        return;
                                    });
                                });
                            } else {
                                utils.log_debug("WRITE_ASYNC(STATEFUL): calling syndicate.write_async");
                                syndicate.write_async(ug, stat.fh, buffer, function(err, data) {
                                    if(err) {
                                        return_error(req, res, err);
                                        return;
                                    }

                                    return_data(req, res, null);
                                    //stat.inc(req, stat.keys.FILE_WRITE);
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
                    var success = gatewayState.extend_file_handle_ttl(gateway_state, fd);
                    if(!success) {
                        throw new Error("could not find a file handle");
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
                    var success = gatewayState.extend_file_handle_ttl(gateway_state, fd);
                    if(!success) {
                        throw new Error("could not find a file handle");
                    }

                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rename !== undefined) {
                // rename: ?rename&to='to_filename'
                var to_name = options.to;
                try {
                    utils.log_debug(util.format("RENAME: calling syndicate.rename - %s to %s", path, to_name));
                    syndicate.rename(ug, path, to_name);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rename_async !== undefined) {
                // rename_async: ?rename_async&to='to_filename'
                var to_name = options.to;
                try {
                    utils.log_debug("RENAME_ASYNC: calling syndicate.rename_async - %s to %s", path, to_name));
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
            } else {
                res.status(403).send();
            }
        };
        router.post('*', authenticate, r_post);

        /*
         * HTTP PUT: write operations
         */
        router.put('*', authenticate, r_post);

        /*
         * HTTP DELETE: unlink operations
         */
        var r_delete = function(req, res) {
            var session_name = req.user;
            var options = req.query;
            var path = extracted.path;
            var session = sessions.get_session(req.sessions, session_name);
            if(session === null) {
                // session not exist
                return_forbidden(req, res, util.format("cannot retrieve a session from a key - %s", session_key));
                return;
            }

            var gateway_state = session.gateway_state;
            if(gateway_state === null) {
                // session not exist
                return_forbidden(req, res, util.format("cannot retrieve a gateawy from a key - %s", session_key));
                return;
            }

            var ug = gateway_state.ug;

            //stat.inc(req, stat.keys.REQUEST_DELETE);

            if(options.rmdir !== undefined) {
                // rmdir: ?rmdir
                try {
                    utils.log_debug(util.format("RMDIR: calling syndicate.rmdir - %s", path));
                    syndicate.rmdir(ug, path);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmdir_async !== undefined) {
                // rmdir_async: ?rmdir_async
                try {
                    utils.log_debug(util.format("RMDIR_ASYNC: calling syndicate.rmdir_async - %s", path));
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
                    utils.log_debug(util.format("UNLINK: calling syndicate.unlink - %s", path));
                    syndicate.unlink(ug, path);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.unlink_async !== undefined) {
                // unlink_async: ?unlink_async
                try {
                    utils.log_debug(util.format("UNLINK_ASYNC: calling syndicate.unlink_async - %s", path));
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
                    utils.log_debug(util.format("RMXATTR: calling syndicate.remove_xattr - %s, %s", path, key));
                    syndicate.remove_xattr(ug, path, key);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.rmxattr_async !== undefined) {
                // rmxattr_async: ?rmxattr_async&key='name'
                var key = options.key;
                try {
                    utils.log_debug(util.format("RMXATTR_ASYNC: calling syndicate.remove_xattr_async - %s, %s", path, key));
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
                    var stat = gatewayState.stat_file_handle(gateway_state, fd);
                    if(stat === null) {
                        throw new Error(util.format("unable to find a file handle for %d", fd));
                    }

                    gatewayState.destroy_file_handle(gateway_state, fd);

                    // write mode
                    if(stat.flag == 'w') {
                        utils.log_debug("CLOSE: calling syndicate.fsync");
                        syndicate.fsync(ug, stat.fh);
                    }

                    utils.log_debug("CLOSE: calling syndicate.close");
                    syndicate.close(ug, stat.fh);
                    //stat.dec(req, stat.keys.FILE_OPENED);
                    return_data(req, res, null);
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else if(options.close_async !== undefined) {
                // close_async: ?close_async&fd=fd
                var fd = options.fd;
                try {
                    var stat = gatewayState.stat_file_handle(gateway_state, fd);
                    if(stat === null) {
                        throw new Error(util.format("unable to find a file handle for %d", fd));
                    }

                    gatewayState.destroy_file_handle(gateway_state, fd);

                    // write mode
                    if(stat.flag == 'w') {
                        utils.log_debug("CLOSE_ASYNC: calling syndicate.fsync_async");
                        syndicate.fsync_async(ug, stat.fh, function(err, data) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            utils.log_debug("CLOSE_ASYNC: calling syndicate.close_async");
                            syndicate.close_async(ug, stat.fh, function(err, data) {
                                if(err) {
                                    return_error(req, res, err);
                                    return;
                                }

                                return_data(req, res, null);
                                //stat.dec(req, stat.keys.FILE_OPENED);
                            });
                        });
                    } else {
                        utils.log_debug("CLOSE_ASYNC: calling syndicate.close_async");
                        syndicate.close_async(ug, stat.fh, function(err, data) {
                            if(err) {
                                return_error(req, res, err);
                                return;
                            }

                            return_data(req, res, null);
                            //stat.dec(req, stat.keys.FILE_OPENED);
                        });
                    }
                } catch (ex) {
                    return_error(req, res, ex);
                }
            } else {
                res.status(403).send();
            }
        };
        router.delete('*', authenticate, r_delete);

        return router;
    }
};
