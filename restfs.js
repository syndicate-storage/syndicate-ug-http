/***********************************************************************************************
 * This part is based on exposefs (https://github.com/gchaincl/exposefs) written by Gustaf Shin.
 ***********************************************************************************************/
var express = require('express');
var path = require('path');
var querystring = require('querystring');

module.exports = function(options) {
    var router = new express.Router();
    if(!options) options = {};
    var backend = options.backend || 'filesystem';

    backend = require('./backends/' + backend)(options);

    router.use(function(req, res, next) {
        req.target = querystring.unescape(req.path);
        next();
    });

    /*
     * HTTP GET: readdir/read/stat/follow operations
     */
    router.get('*', function(req, res) {
        var options = req.query;

        if(options.stat !== undefined) {
            // stat: ?stat
            backend.stat(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.getxattr !== undefined) {
            // getxattr: ?getxattr&key='name'
            backend.getxattr(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.listxattr !== undefined) {
            // listxattr: ?listxattr
            backend.listxattr(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.readdir !== undefined) {
            // readdir: ?readdir
            backend.readdir(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.readdirwithstat !== undefined) {
            // readdirwithstat: ?readdirwithstat
            backend.readdirwithstat(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.readfully !== undefined) {
            // readfully: ?readfully
            backend.readfully(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.open !== undefined) {
            // open: ?open&flags='r'&mode=777
            backend.open(req.target, options, function(err, data) {
                res.send(err || {fd:data});
            });
        } else if(options.read !== undefined) {
            // read: ?read&fd=fd&offset=offset&length=len
            backend.read(req.target, options, function(err, bytesRead, data) {
                res.send(err || data.slice(0, bytesRead));
            });
        } else if(options.follow !== undefined) {
            // follow: ?follow
            backend.readfully(req.target, options, function(err, data) {
                res.writeHead(200, {
                    'Content-Type': 'text/event-stream',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                });

                res.write(data, "\n");

                fn = backend.follow(req.target, options, res);
                res.socket.on('close', fn);
            });
        } else {
            res.status(404).send();
        }
    });

    /*
     * HTTP POST: write/mkdir operations
     */
    router.post('*', function(req, res) {
        var options = req.query;

        if(options.mkdir !== undefined) {
            // mkdir: ?mkdir&mode=777
            backend.mkdir(req.target, options, function(err, data) {
                if(err)
                    res.status(401).send(err);
                else
                    res.status(201).send(data);
            });
        } else if(options.writefully !== undefined) {
            // writefully: ?writefully
            backend.writefully(req.target, req, options, function(err, data) {
                if(err)
                    res.status(401).send(err);
                else
                    res.status(201).send(data);
            });
        } else if(options.setxattr !== undefined) {
            // setxattr: ?setxattr&key='name'&val='val'
            backend.setxattr(req.target, options, function(err, data) {
                if(err)
                    res.status(401).send(err);
                else
                    res.status(201).send(data);
            });
        } else if(options.write !== undefined) {
            // write: ?write&fd=fd&offset=offset&length=len
            backend.write(req.target, req, options, function(err, bytesWritten, data) {
                if(err)
                    res.status(401).send(err);
                else
                    res.status(201).send({written:bytesWritten});
            });
        } else {
            res.status(404).send();
        }
    });

    /*
     * HTTP PUT: write operations
     */
    router.put('*', function(req, res) {
        var options = req.query;

        if(options.writefully !== undefined) {
            // writefully: ?writefully
            backend.writefully(req.target, req, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.setxattr !== undefined) {
            // setxattr: ?setxattr&key='name'&val='val'
            backend.setxattr(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.write !== undefined) {
            // write: ?write&fd=fd&offset=offset&length=len
            backend.write(req.target, req, options, function(err, bytesWritten, data) {
                res.send(err || {written:bytesWritten});
            });
        } else {
            res.status(404).send();
        }
    });

    /*
     * HTTP PATCH: utimes operations
     */
    router.patch('*', function(req, res) {
        var options = req.query;

        if(options.utimes !== undefined) {
            // utimes: ?utimes&time=new_time
            backend.utimes(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.rename !== undefined) {
            // rename: ?rename&to='to_filename'
            backend.rename(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.truncate !== undefined) {
            // truncate: ?truncate&offset=offset
            backend.truncate(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else {
            res.status(404).send();
        }
    });

    /*
     * HTTP DELETE: unlink operations
     */
    router.delete('*', function(req, res) {
        var options = req.query;

        if(options.rmdir !== undefined) {
            // rmdir: ?rmdir
            backend.rmdir(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.unlink !== undefined) {
            // unlink: ?unlink
            backend.unlink(req.target, req, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.rmxattr !== undefined) {
            // rmxattr: ?rmxattr&key='name'
            backend.rmxattr(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.close !== undefined) {
            // close: ?close&fd=fd
            backend.close(req.target, options, function(err, data) {
                res.send(err || {fd:data});
            });
        } else {
            res.status(404).send();
        }
    });

    return router;
};
