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

        // stat: with ?stat
        if(options.stat !== undefined) {
            backend.stat(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.readdir !== undefined) {
            backend.readdir(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.readdirwithstat !== undefined) {
            backend.readdirwithstat(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.readfully !== undefined) {
            backend.readfully(req.target, options, function(err, data) {
                res.send(err || data);
            });
        } else if(options.follow !== undefined) {
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
            // default
            backend.read(req.target, options, function(err, data) {
                res.send(err || data);
            });
        }
    });

    /*
     * HTTP POST: write/mkdir operations
     */
  router.post('*', function(req, res) {
    var target = req.target;
    var options = req.query;

    if(target.charAt(target.length - 1) == "/") {
      options.directory = true;
      target = target.substr(0, target.length - 1);
    }

    backend.create(target, req, options, function(err, data) {
      if(err)
        res.status(401).send(err);
      else
        res.status(201).send(data);
    });
  });

  router.put('*', function(req, res) {
    var options = req.query;

    backend.write(req.target, req, options, function(err, data) {
      res.send(err || data);
    });
  });

  router.patch('*', function(req, res) {
    var options = req.query;

    backend.utimes(req.target, options, function(err, data) {
      res.send(err || data);
    });
  });

  router.delete('*', function(req, res) {
    var options = req.query;

    backend.unlink(req.target, options, function(err, data) {
      res.send(err || data);
    });
  });

  return router;
};
