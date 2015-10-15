/*
#Authors of Node-FSAPI

Kent Safranski - @fluidbyte <kent@fluidbyte.net>
*/
/*
Copyright (c) 2013 Node-FSAPI

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

// Define Configuration
// now we gets this from outside
// below is default
var config = {
    /**
     * Allowed IP's or ranges
     * Can use * for wildcards, *.*.*.* for no restrictions
     */
    ips: [
        "*.*.*.*"
    ],
    /**
     * SSL Config
     * Set key and cert to absolute path if SSL used, false if not
     */
    ssl: {
        key: false,
        cert: false
    },
    // Port designation
    port: 8080,
    // Base directory
    base: "example/base",
    // Default create mode
    cmode: "0755",
    // Default service name
    servicename: "fsapi"
};


var fs = require("fs-extra"),
    restify = require("restify"),
    server;

// Regular Expressions
var commandRegEx = /^\/([a-zA-Z0-9_\.~-]+)\/(.*)/,  // /{command}/{path}
    pathRegEx = /^\/(.*)/;  // /{path}


/**
 * UnknownMethod handler
 */
function unknownMethodHandler(req, res) {
  if (req.method.toLowerCase() === 'options') {
    var allowHeaders = ['Accept', 'Accept-Version', 'Content-Type', 'Api-Version', 'Origin', 'X-Requested-With']; // added Origin & X-Requested-With

    if (res.methods.indexOf('OPTIONS') === -1) res.methods.push('OPTIONS');

    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
    res.header('Access-Control-Allow-Origin', req.headers.origin);

    return res.send(204);
  }
  else
    return res.send(new restify.MethodNotAllowedError());
}

/**
 * Check IP (Called by checkReq)
 */
 
var checkIP = function (config, req) {
    var ip = req.connection.remoteAddress.split("."),
        curIP,
        b,
        block = [];
    for (var i=0, z=config.ips.length-1; i<=z; i++) {
        curIP = config.ips[i].split(".");
        b = 0;
        // Compare each block
        while (b<=3) {
            (curIP[b]===ip[b] || curIP[b]==="*") ? block[b] = true : block[b] = false;
            b++;
        }
        // Check all blocks
        if (block[0] && block[1] && block[2] && block[3]) {
            return true;
        }
    }
    return false;
};
 

/**
 * Check Request
 * Checks IP Address
 */
 
var checkReq = function (config, req, res) {
    
    // Set access control headers
    res.header('Access-Control-Allow-Origin', '*');
    
    // Check and IP
    if(!checkIP(config, req)) {
        res.send(401);
        return false;
    }
    
    return true;
};

/**
 * Response Error
 */
 
var resError = function (code, raw, res) {
    
    var codes = {
        100: "Unknown command",
        101: "Could not list files",
        102: "Could not read file",
        103: "Path does not exist",
        104: "Could not create copy",
        105: "File does not exist",
        106: "Not a file",
        107: "Could not write to file",
        108: "Could not delete object"
    };
    
    res.send({ "status": "error", "code": code, "message": codes[code], "raw": raw });
    return false;
    
};

/**
 * Response Success
 */
 
var resSuccess = function (data, res) {

    res.send({ "status": "success", "data": data });

};

/**
 * Merge function
 */

var merge = function (obj1,obj2) {
    var mobj = {},
        attrname;
    for (attrname in obj1) { mobj[attrname] = obj1[attrname]; }
    for (attrname in obj2) { mobj[attrname] = obj2[attrname]; }
    return mobj;
};

/**
 * Get Base Path
 */

var getBasePath = function (path) {
    var base_path = path.split("/");
        
    base_path.pop();
    return base_path.join("/");
};

/**
 * Check Path
 */
 
var checkPath = function (path) {
    var base_path = getBasePath(path);
    return fs.existsSync(base_path);
};

/**
 * GET (Read)
 * 
 * Commands:
 * dir - list contents of directory
 * file - return content of a file
 * 
 */
function get (req, res, next) {
    
    // Check request
    checkReq(config, req, res);
    
    // Set path
    var path = config.base + "/" + req.params[1];
    
    switch (req.params[0]) {
        // List contents of directory
        case "dir":
            fs.readdir(path, function (err, files) {
                if (err) {
                    resError(101, err, res);
                } else {
                    
                    // Ensure ending slash on path
                    (path.slice(-1)!=="/") ? path = path + "/" : path = path;
                
                    var output = {},
                        output_dirs = {},
                        output_files = {},
                        current,
                        relpath,
                        link;

                    // Function to build item for output objects
                    var createItem = function (current, relpath, type, link) {
                        return {
                            path: relpath.replace('//','/'),
                            type: type,
                            size: fs.lstatSync(current).size,
                            atime: fs.lstatSync(current).atime.getTime(),
                            mtime: fs.lstatSync(current).mtime.getTime(),
                            link: link
                        };
                    };
                    
                    // Sort alphabetically
                    files.sort();
                    
                    // Loop through and create two objects
                    // 1. Directories
                    // 2. Files
                    for (var i=0, z=files.length-1; i<=z; i++) {
                        current = path + files[i];
                        relpath = current.replace(config.base,"");
                        (fs.lstatSync(current).isSymbolicLink()) ? link = true : link = false;
                        if (fs.lstatSync(current).isDirectory()) {
                            output_dirs[files[i]] = createItem(current,relpath,"directory",link);
                        } else {
                            output_files[files[i]] = createItem(current,relpath,"file",link);                            
                        }
                    }
                    
                    // Merge so we end up with alphabetical directories, then files
                    output = merge(output_dirs,output_files);
                    
                    // Send output
                    resSuccess(output, res);
                }
            });
            break;
        
        // Return contents of requested file
        case "file":
            fs.readFile(path, "utf8", function (err, data) {
                if (err) {
                    resError(102, err, res);
                } else {
                    resSuccess(data, res);
                }
            });
            break;
            
        default:
            // Unknown command
            resError(100, null, res);
    }

    return next();
}

/**
 * POST (Create)
 * 
 * Commands:
 * dir - creates a new directory
 * file - creates a new file
 * copy - copies a file or dirextory (to path at param "destination")
 * 
 */
function post(req, res, next) {
    
    // Check request
    checkReq(config, req, res);
    
    // Set path
    var path = config.base + "/" + req.params[1];
    
    switch (req.params[0]) {
        
        // Creates a new directory
        case "dir":
            
            // Ensure base path
            if (checkPath(path)) {
                // Base path exists, create directory
                fs.mkdir(path, config.cmode, function () {
                    resSuccess(null, res);
                });
            } else {
                // Bad base path
                resError(103, null, res);
            }
            break;
        
        // Creates a new file
        case "file":
            // Ensure base path
            if (checkPath(path)) {
                // Base path exists, create file
                fs.openSync(path, "w");
                resSuccess(null, res);
            } else {
                // Bad base path
                resError(103, null, res);
            }
            break;
        
        // Copies a file or directory
        // Supply destination as full path with file or folder name at end
        // Ex: http://yourserver.com/copy/folder_a/somefile.txt, destination: /folder_b/somefile.txt
        case "copy":
            var destination = config.base + "/" + req.params.destination;
            if (checkPath(path) && checkPath(destination)) {
                fs.copy(path, destination, function(err){
                    if (err) {
                        resError(104, err, res);
                    }
                    else {
                        resSuccess(null, res);
                    }
                });
            } else {
                // Bad base path
                resError(103, null, res);
            }
            break;
            
        default:
            // Unknown command
            resError(100, null, res);
    }
    
    return next();
}

/**
 * PUT (Update)
 * 
 * Commands:
 * rename - renames a file or folder (using param "name")
 * save - saves contents to a file (using param "data")
 * 
 */
function put (req, res, next) {
    
    // Check request
    checkReq(config, req, res);
    
    // Set path
    var path = config.base + "/" + req.params[1];
    
    switch (req.params[0]) {
        
        // Rename a file or directory
        case "rename":
            
            var base_path = getBasePath(path);
            
            fs.rename(path,base_path + "/" + req.params.name, function () {
                resSuccess(null, res);
            });
            
            break;
        
        // Saves contents to a file
        case "save":
            
            // Make sure it exists
            if (fs.existsSync(path)) {
                // Make sure it's a file
                if (!fs.lstatSync(path).isDirectory()) {
                    // Write
                    fs.writeFile(path, req.params.data, function(err) {
                        if(err) {
                            resError(107, err, res);
                        } else {
                            resSuccess(null, res);
                        }
                    });
                } else {
                    resError(106, null, res);
                }
            } else {
                resError(105, null, res);
            }
            
            break;
        
            
        default:
            // Unknown command
            resError(100, null, res);
    }
    
    return next();
    
}

/**
 * DELETE 
 */
function del (req, res, next) {
    
    // Check request
    checkReq(config, req, res);
    
    // Set path
    var path = config.base + "/" + req.params[0];
    
    // Make sure it exists
    if (fs.existsSync(path)) {
        // Remove file or directory
        fs.remove(path, function (err) {
            if (err) {
                resError(108, err, res);
            } else {
                resSuccess(null, res);
            }
        });
    } else {
        resError(103, null, res);
    }
    
    return next();
    
}

/**
 * START SERVER
 */
exports.start = function start(cfg) {
    if(cfg != null) {
        if(cfg.ips != null) {
            config.ips = cfg.ips;
        }

        if(cfg.ssl != null) {
            config.ssl = cfg.ssl;
        }

        if(cfg.port != null) {
            config.port = cfg.port;
        }

        if(cfg.base != null) {
            config.base = cfg.base;
        }

        if(cfg.cmode != null) {
            config.cmode = cfg.cmode;
        }

        if(cfg.servicename != null) {
            config.servicename = cfg.servicename;
        }
    }

    // Determine if SSL is used
    if (config.ssl.key && config.ssl.cert) {
        // Get CERT
        var https = {
            certificate: fs.readFileSync(config.ssl.cert),
            key: fs.readFileSync(config.ssl.key)
        };
        
        // Config server with SSL
        server = restify.createServer({
            name: config.servicename,
            certificate: https.certificate,
            key: https.key
        });
    } else {
        // Config non-SSL Server
        server = restify.createServer({
            name: config.servicename
        });
    }

    // Additional server config
    server.use(restify.acceptParser(server.acceptable));
    server.use(restify.queryParser());
    server.use(restify.bodyParser());
    server.use(restify.CORS());

    server.on('MethodNotAllowed', unknownMethodHandler);

    // Add handlers
    server.del(pathRegEx, del);
    server.put(commandRegEx, put);
    server.post(commandRegEx, post);
    server.get(commandRegEx, get);

    // Start
    server.listen(config.port, function () {
        console.log("%s listening at %s", server.name, server.url);
    });
}
