'use strict';

// Module imports
const express = require('express')
    , https = require('https')
    , qs = require('querystring')
    , async = require('async')
    , _ = require('lodash')
    , uuid = require('shortid')
    , restify = require('restify-clients')
    , log = require('npmlog-ts')
    , config = require('config')
    , fs = require('fs')
;

// ************************************************************************
// Main code STARTS HERE !!
// ************************************************************************

const options = {
  cert: fs.readFileSync("/u01/ssl/certificate.fullchain.crt").toString(),
  key: fs.readFileSync("/u01/ssl/certificate.key").toString()
};

log.level = 'verbose';
log.stream = process.stdout;
log.timestamp = true;

const VERSION = "1.0"
;

const PROCESS   = "PROCESS"
    , REST      = "REST"
    , WEBSOCKET = "WEBSOCKET"
    , STREAMING = "STREAMING"
    , DB        = "DB"
;

// Instantiate classes & servers
const DBHOST                = "https://apex.wedoteam.io"
    , OCIBRIDGEHOST         = "https://local.infra.wedoteam.io:2443"
    , OCIBRIDGEUSERNAME     = config.get('ociwrapper.username')
    , OCIBRIDGEPASSWORD     = config.get('ociwrapper.password')
    , DBURI                 = '/ords/pdb1/wedo/common'
    , STREAMINGSETUP        = '/streaming/setup'
    , STREAMINGCREATECURSOR = '/20180418/streams/{streamid}/cursors'
    , STREAMINGPOOLMESSAGES = '/20180418/streams/{streamid}/messages'
    , POOLINGINTERVAL       = 1000
    , PINGINTERVAL          = 25000
    , PINGTIMEOUT           = 60000
;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info(PROCESS,"Uncaught Exception: " + err);
  log.info(PROCESS,"Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info(PROCESS,"Caught interrupt signal");
  log.info(PROCESS,"Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

// Initializing REST client BEGIN
var dbClient = restify.createJsonClient({
  url: DBHOST,
  connectTimeout: 10000,
  requestTimeout: 10000,
  retry: false,
  rejectUnauthorized: false,
  headers: {
    "content-type": "application/json",
    "accept": "application/json"
  }
});
// Initializing REST client END

var demozones = _.noop();

async.series( {
  splash: (next) => {
    log.info(PROCESS, "WEDO OCI Streaming - WebSockets Bridge - " + VERSION);
    log.info(PROCESS, "Author: Carlos Casares <carlos.casares@oracle.com>");
    next();
  },
  setup: (next) => {
    log.verbose(DB, "Retrieving streaming setup for all demozones");
    dbClient.get(DBURI + STREAMINGSETUP, (err, req, res, obj) => {
      if (err) {
        log.verbose(DB, "Error retrieving setup: %s", err.message);
        next(err);
        return;
      }
      var jBody = JSON.parse(res.body);
      demozones = _.cloneDeep(jBody.items);
      next();
    });
  },
  websocket: (next) => {
    async.eachSeries( demozones, (d, nextDemozone) => {
      var i = 0;
      d.interval = _.noop();
      d.cursor  = _.noop();
      d.running = false;
      d.app = express();
      d.server = https.createServer(options, d.app);
      d.ociBridgeClient = restify.createJsonClient({
        url: OCIBRIDGEHOST,
        connectTimeout: 10000,
        requestTimeout: 10000,
        retry: false,
        rejectUnauthorized: false,
        headers: {
          "content-type": "application/json",
          "accept": "application/json",
          "wedo-service-uri": d.serviceuri
        }
      });
      d.ociBridgeClient.basicAuth(OCIBRIDGEUSERNAME, OCIBRIDGEPASSWORD);
      d.io = require('socket.io')(d.server, {'pingInterval': PINGINTERVAL, 'pingTimeout': PINGTIMEOUT});
      d.io.on('connection', (socket) => {
        log.info(d.demozone,"New client connected. Total number of clients: " + d.io.sockets.server.engine.clientsCount);
        socket.conn.on('heartbeat', () => {
//          log.verbose(d.demozone,'heartbeat');
        });
        socket.on('error', function (err) {
          log.error(d.demozone,"Error: " + err);
        });
        socket.on('disconnect', () => {
          log.info(d.demozone ,"Client disconnected");
        });
        if (d.io.sockets.server.engine.clientsCount > 0) {
          if (!d.interval) {
            log.info(d.demozone,"Starting message pooling interval");
            d.interval = setInterval((s) => {
              if (s.running == true) {
                // Previous interval is still running. Exit.
                log.verbose(STREAMING,"ignoring...");
                return;
              }
              s.running = true;
              if (s.io.sockets.server.engine.clientsCount == 0 && s.interval) {
                log.verbose(d.demozone,"No opened sessions left, clearing message pooling interval");
                clearInterval(d.interval);
                d.interval = _.noop();
                s.cursor = _.noop();
                s.running = false;
                return;
              };
              var messages = [];
              async.series({
                cursor: (nextStreaming) => {
                  if (!s.cursor) {
                    // No cursor, so we need to create one
                    log.verbose(STREAMING,"No cursors available");
                    let body = { partition: "0", type: "LATEST" };
                    s.ociBridgeClient.post(STREAMINGCREATECURSOR.replace('{streamid}', s.streamid), body, (err, req, res, data) => {
                      if (err) {
                        nextStreaming(err.message);
                        return;
                      } else if (res.statusCode == 200) {
                        s.cursor = data.value;
                        log.verbose(STREAMING,"Cursor successfully created");
                        nextStreaming();
                      } else {
                        nextStreaming("Error creating cursor: " + res.statusCode);
                      }
                    });
                  } else {
                    nextStreaming();
                  }
                },
                retrieveMessages: (nextStreaming) => {
//                  log.verbose(STREAMING,"Fetching messages...");
                  s.ociBridgeClient.get(STREAMINGPOOLMESSAGES.replace('{streamid}', s.streamid) + "?" + qs.stringify({ cursor: s.cursor }), (err, req, res, data) => {
                    if (err) {
                      // Let's get rid of the cursor just in case
                      s.cursor = _.noop();
                      nextStreaming(err.message);
                    } else if (res.statusCode == 200) {
                      if (res.headers["opc-next-cursor"]) {
                        s.cursor = res.headers["opc-next-cursor"];
                      }
//                      log.verbose(STREAMING,"Fetching " + data.length + " messages");
                      if (data.length > 0) {
                        log.verbose(STREAMING,"Retrieved " + data.length + " messages");
                        _.each(data, (m) => {
                          let msg = {
                            key: Buffer.from(m.key, 'base64').toString(),
                            value: Buffer.from(m.value, 'base64').toString(),
                          };
                          messages.push(msg);
                        });
                      }
                      nextStreaming();
                    } else {
                      // Invalid cursor?
                      log.error(STREAMING,"Error retrieving messages: " + res.statusCode + ", :" + data);
                      s.cursor = _.noop();
                      nextStreaming();
                    }
                  });
                },
                sendMessages: (nextStreaming) => {
                  if (messages.length > 0) {
                    _.each(messages, (message) => {
                      // Emit message to all connected clients
                      s.io.sockets.emit('message', JSON.stringify(message));
                      log.verbose(STREAMING,"Messages successfully emitted...");
                    });
                  }
                  nextStreaming();
                }
              }, (err, results) => {
                if (err) {
                  log.error("Error during streaming process: " + err);
                }
                s.running = false;
              });
            }, POOLINGINTERVAL, d);
          };
        }
      });
      d.server.listen(d.websocketport, () => {
        log.info(WEBSOCKET,"Created WS server at port: " + d.websocketport + " for demozone: " + d.demozone);
        next();
      });
    }, (err) => {
      next(err);
    });
  }
}, (err, results) => {
  if (err) {
    log.error("Error during initialization: " + err);
  }
});
