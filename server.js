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

// Instantiate classes & servers
const wsURI     = '/socket.io'
    , restURI   = '/event/:eventname';

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

const DBHOST                = "https://apex.wedoteam.io"
    , OCIBRIDGEHOST         = "https://local.infra.wedoteam.io:2443"
    , OCIBRIDGEUSERNAME     = config.get('ociwrapper.username')
    , OCIBRIDGEPASSWORD     = config.get('ociwrapper.password')
    , DBURI                 = '/ords/pdb1/wedo/common'
    , STREAMINGSETUP        = '/streaming/setup'
    , STREAMINGCREATECURSOR = '/20180418/streams/{streamid}/cursors'
    , STREAMINGPOOLMESSAGES = '/20180418/streams/{streamid}/messages'
    , POOLINGINTERVAL       = 100000
;

const PINGINTERVAL = 25000
    , PINGTIMEOUT  = 60000
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
      d.sessions = [];
      d.io = require('socket.io')(d.server, {'pingInterval': PINGINTERVAL, 'pingTimeout': PINGTIMEOUT});
      d.io.on('connection', (socket) => {

        console.log("++++++++++++++++++++++++++++++++++++");
        console.log(d.io.sockets.server.engine);
        console.log(d.io.sockets.adapter.rooms);
        console.log("++++++++++++++++++++++++++++++++++++");


        var sessionUUID = uuid.generate();
        socket.uuid = sessionUUID;
        log.info(d.demozone,"Client connected with UUID: " + socket.uuid);
        socket.conn.on('heartbeat', () => {
          log.verbose(d.demozone + "-" + socket.uuid,'heartbeat');
        });
        d.sessions.push({ uuid: socket.uuid, socket: socket});
        socket.on('disconnect', () => {
          log.info(d.demozone + "-" + socket.uuid,"Socket disconnected");
          // remove session from array
          _.remove(d.sessions, { uuid: socket.uuid });
          log.verbose(d.demozone ,"Remaining opened sessions: " + d.sessions.length);
          if (d.sessions.length == 0 && d.interval) {
            log.verbose(d.demozone,"No opened sessions left, clearing message pooling interval");
            clearInterval(d.interval);
            d.interval = _.noop();
          };
        });
        socket.on('error', function (err) {
          log.error(d.demozone + "-" + socket.uuid,"Error: " + err);
        });
        if (!d.interval) {
          log.info(d.demozone,"Starting message pooling interval");
          d.interval = setInterval((s) => {
            if (s.running == true) {
              // Previous interval is still running. Exit.
              return;
            }
            s.running = true;
            let messages = [];
            async.series({
              cursor: (nextStreaming) => {
                if (!s.cursor) {
                  // No cursor, so we need to create one
                  log.verbose(STREAMING,"No cursors available");
                  let body = { partition: "0", type: "LATEST" };
                  d.ociBridgeClient.post(STREAMINGCREATECURSOR.replace('{streamid}', d.streamid), body, (err, req, res, data) => {
                    if (err) {
                      nextStreaming(err.message);
                      return;
                    } else if (res.statusCode == 200) {
                      s.cursor = data.value;
                      nextStreaming();
                    } else {
                      nextStreaming("Error creating cursor: " + res.statusCode);
                    }
                  });
                }
              },
              retrieveMessages: (nextStreaming) => {
                d.ociBridgeClient.get(STREAMINGPOOLMESSAGES.replace('{streamid}', d.streamid) + "?" + qs.stringify({ cursor: d.cursor }), body, (err, req, res, data) => {
                  if (err) {
                    nextStreaming(err.message);
                  } else if (res.statusCode == 200) {
                    if (data.length > 0) {
                      log.verbose(STREAMING,"Retrieved " + data.lebgth + " messages");
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
                    d.cursor = _.noop();
                    nextStreaming();
                  }
                });
              },
              sendMessages: (nextStreaming) => {
                if (messages.length > 0 && s.sessions.length > 0) {
                  _.each(s.sessions, (session) => {
                    _.each(messages, (message) => {
                      session.socket.emit('message', JSON.stringify())
                    })
                  });
                  nextStreaming();
                }
              }
            }, (err, results) => {
              if (err) {
                log.error("Error during streaming process: " + err);
              }
              s.running = false;
            });
          }, POOLINGINTERVAL, d);
        };
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

async function getMessages(d) {
  var promise = new Promise((resolve, reject) => {
  });
  let result = await promise;
  return result;
};

//async function createCursor(d)
function createCursor(d) {
  console.log("2");
//  var promise = new Promise((resolve, reject) => {
  return new Promise((resolve, reject) => {
    console.log("3");
  });
  /**
  console.log("5");
  let result = await promise;
  console.log("6");
  return result;
  **/
};
