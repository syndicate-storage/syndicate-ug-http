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

var ug = null;

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

            if(options.stat !== undefined) {
                // stat: ?stat
                try {
                    // here?
                    var ret = syndicate.stat_raw(ug, path);
                    res.status(200).send(ret);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(ex.toString());
                }
            } else if(options.listdir !== undefined) {
                // stat: ?listdir
                try {
                    var ret = syndicate.list_dir(ug, path);
                    res.status(200).send(ret);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(ex.toString());
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
                    res.status(500).send(ex.toString());
                }
            } else if(options.listxattr !== undefined) {
                // listxattr: ?listxattr
                try {
                    var ret = syndicate.list_xattr(ug, path);
                    res.status(200).send(ret);
                } catch (ex) {
                    console.error("Exception occured : " + ex);
                    res.status(500).send(ex.toString());
                }
            /*
            } else if(options.readdirwithstat) {
                // readdirwithstat: ?readdirwithstat
                syndicatefs.readdirwithstat(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(200).send(data);
                });
            } else if(options.readfully) {
                // readfully: ?readfully
                syndicatefs.readfully(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(200).send(data);
                });
            } else if(options.open) {
                // open: ?open&flags='r'&mode=777
                syndicatefs.open(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(200).send({fd:data});
                });
            } else if(options.read) {
                // read: ?read&fd=fd&offset=offset&length=len
                syndicatefs.read(req.target, options, function(err, bytesRead, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(200).send(data.slice(0, bytesRead));
                });
            } else if(options.follow) {
                // follow: ?follow
                syndicatefs.readfully(req.target, options, function(err, data) {
                    res.writeHead(200, {
                        'Content-Type': 'text/event-stream',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    });

                    res.write(data, "\n");

                    fn = syndicatefs.follow(req.target, options, res);
                    res.socket.on('close', fn);
                });
            */
            } else {
                res.status(403).send();
            }
        });

        /*
         * HTTP POST: write/mkdir operations
         */
        /*
        router.post('*', function(req, res) {
            var options = req.query;

            if(options.mkdir !== undefined) {
                // mkdir: ?mkdir&mode=777
                syndicatefs.mkdir(req.target, options, function(err, data) {
                    if(err)
                        res.status(401).send(err);
                    else
                        res.status(201).send(data);
                });
            } else if(options.writefully !== undefined) {
                // writefully: ?writefully
                syndicatefs.writefully(req.target, req, options, function(err, data) {
                    if(err)
                        res.status(401).send(err);
                    else
                        res.status(201).send(data);
                });
            } else if(options.setxattr !== undefined) {
                // setxattr: ?setxattr&key='name'&val='val'
                syndicatefs.setxattr(req.target, options, function(err, data) {
                    if(err)
                        res.status(401).send(err);
                    else
                        res.status(201).send(data);
                });
            } else if(options.write !== undefined) {
                // write: ?write&fd=fd&offset=offset&length=len
                syndicatefs.write(req.target, req, options, function(err, bytesWritten, data) {
                    if(err)
                        res.status(401).send(err);
                    else
                        res.status(201).send({written:bytesWritten});
                });
            } else if(options.utimes !== undefined) {
                // utimes: ?utimes&time=new_time
                syndicatefs.utimes(req.target, options, function(err, data) {
                    res.send(err || data);
                });
            } else if(options.rename !== undefined) {
                // rename: ?rename&to='to_filename'
                syndicatefs.rename(req.target, options, function(err, data) {
                    res.send(err || data);
                });
            } else if(options.truncate !== undefined) {
                // truncate: ?truncate&offset=offset
                syndicatefs.truncate(req.target, options, function(err, data) {
                    res.send(err || data);
                });
            } else {
                res.status(403).send();
            }
        });
        */

        /*
         * HTTP PUT: write operations
         */
        /*
        router.put('*', function(req, res) {
            var options = req.query;

            if(options.writefully !== undefined) {
                // writefully: ?writefully
                syndicatefs.writefully(req.target, req, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.setxattr !== undefined) {
                // setxattr: ?setxattr&key='name'&val='val'
                syndicatefs.setxattr(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.write !== undefined) {
                // write: ?write&fd=fd&offset=offset&length=len
                syndicatefs.write(req.target, req, options, function(err, bytesWritten, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send({written:bytesWritten});
                });
            } else if(options.utimes !== undefined) {
                // utimes: ?utimes&time=new_time
                syndicatefs.utimes(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.rename !== undefined) {
                // rename: ?rename&to='to_filename'
                syndicatefs.rename(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.truncate !== undefined) {
                // truncate: ?truncate&offset=offset
                syndicatefs.truncate(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else {
                res.status(403).send();
            }
        });
        */

        /*
         * HTTP PATCH: utimes operations
         */
        /*
        router.patch('*', function(req, res) {
            var options = req.query;

            if(options.utimes !== undefined) {
                // utimes: ?utimes&time=new_time
                syndicatefs.utimes(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.rename !== undefined) {
                // rename: ?rename&to='to_filename'
                syndicatefs.rename(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.truncate !== undefined) {
                // truncate: ?truncate&offset=offset
                syndicatefs.truncate(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else {
                res.status(403).send();
            }
        });
        */
        /*
         * HTTP DELETE: unlink operations
         */
        /*
        router.delete('*', function(req, res) {
            var options = req.query;

            if(options.rmdir !== undefined) {
                // rmdir: ?rmdir
                syndicatefs.rmdir(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.unlink !== undefined) {
                // unlink: ?unlink
                syndicatefs.unlink(req.target, req, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.rmxattr !== undefined) {
                // rmxattr: ?rmxattr&key='name'
                syndicatefs.rmxattr(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send(data);
                });
            } else if(options.close !== undefined) {
                // close: ?close&fd=fd
                syndicatefs.close(req.target, options, function(err, data) {
                    if(err)
                        res.status(500).send(err);
                    else
                        res.status(202).send({fd:data});
                });
            } else {
                res.status(403).send();
            }
        });
        */
        return router;
    }
};
