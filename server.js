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
    , POOLINGINTERVAL       = 1000
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
            if (!s.cursor) {
              // No cursor, so we need to create one
              log.verbose(STREAMING,"No cursors available");
              let result = createCursor(s);
              if (!result.value) {
                log.error(STREAMING, "Error creating cursor: " + JSON.stringify(result));
                s.running = false;
                return;
              }
              s.cursor = result.value;
            }
            // First try to get messages
            let result = getMessages(s);
            console.log(result);
            s.running = false;
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
    d.ociBridgeClient.get(STREAMINGPOOLMESSAGES.replace('{streamid}', d.streamid) + "?" + qs.stringify({ cursor: d.cursor }), body, (err, req, res, data) => {
      if (err) {
        reject(err);
      } else if (res.statusCode == 200) {
        resolve(data);
      } else {
        reject(res);
      }
    });
  });
  let result = await promise;
  return result;
};

async function createCursor(d) {
  var promise = new Promise((resolve, reject) => {
    let body = { partition: "0", type: "LATEST" };
    d.ociBridgeClient.post(STREAMINGCREATECURSOR.replace('{streamid}', d.streamid), body, (err, req, res, data) => {
      console.log("returned");
      if (err) {
        console.log("error");
        console.log(err);
        reject(err);
      } else if (res.statusCode == 200) {
        console.log("ok");
        console.log(data);
        resolve(data);
      } else {
        console.log("ok with error");
        reject(res);
      }
    });
  });
  console.log("1");
  let result = await promise;
  console.log("2");
  return result;
};
