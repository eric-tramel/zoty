/* Zoty Bridge — bootstrap plugin for Zotero 7 and 8.
 *
 * Exposes a minimal HTTP endpoint on localhost that accepts JavaScript code
 * via POST and evaluates it inside Zotero's privileged context. This lets
 * external tools (like the zoty MCP server) call Zotero.Attachments,
 * Zotero.Items, etc. without needing direct database access.
 *
 * Only binds to 127.0.0.1 — not accessible from the network.
 */

const DEFAULT_PORT = 24119;
const PREF_ENABLED = "extensions.zoty-bridge.enabled";
const PREF_PORT = "extensions.zoty-bridge.port";

let serverSocket = null;

function log(msg) {
  Zotero.debug("[zoty-bridge] " + msg);
}

function getPort() {
  try {
    var p = Zotero.Prefs.get(PREF_PORT);
    return typeof p === "number" && p > 0 ? p : DEFAULT_PORT;
  } catch (_) {
    return DEFAULT_PORT;
  }
}

function isEnabled() {
  try {
    return Zotero.Prefs.get(PREF_ENABLED) !== false;
  } catch (_) {
    return true;
  }
}

function sendHTTP(output, status, body) {
  var statusText = status === 200 ? "OK" : status === 400 ? "Bad Request"
    : status === 404 ? "Not Found" : "Internal Server Error";
  var resp = "HTTP/1.0 " + status + " " + statusText + "\r\n"
    + "Content-Type: application/json\r\n"
    + "Content-Length: " + body.length + "\r\n"
    + "Connection: close\r\n"
    + "\r\n"
    + body;
  try {
    output.write(resp, resp.length);
    output.close();
  } catch (e) {
    log("Write error: " + e);
  }
}

function handleRequest(data, output) {
  try {
    // Parse first line for method and path
    var firstLine = data.split("\r\n")[0] || "";
    var parts = firstLine.split(" ");
    var method = parts[0];
    var path = parts[1];

    // Extract body after blank line
    var bodyIdx = data.indexOf("\r\n\r\n");
    var body = bodyIdx !== -1 ? data.substring(bodyIdx + 4) : "";

    if (method === "GET" && path === "/status") {
      sendHTTP(output, 200, JSON.stringify({ status: "ok", version: "0.2.0" }));
      return;
    }

    if (method === "POST" && path === "/execute") {
      var code;
      try {
        var parsed = JSON.parse(body);
        code = parsed.code;
      } catch (_) {
        code = body;
      }

      if (!code) {
        sendHTTP(output, 400, JSON.stringify({ error: "no code provided" }));
        return;
      }

      try {
        var fn = new Function("Zotero", "return (async () => { " + code + " })();");
        var promise = fn(Zotero);

        promise.then(
          function(result) {
            sendHTTP(output, 200, JSON.stringify({ ok: true, result: result }));
          },
          function(err) {
            sendHTTP(output, 200, JSON.stringify({ ok: false, error: err.toString() }));
          }
        );
      } catch (e) {
        sendHTTP(output, 200, JSON.stringify({ ok: false, error: e.toString() }));
      }
      return;
    }

    sendHTTP(output, 404, JSON.stringify({ error: "not found" }));
  } catch (e) {
    log("Request error: " + e);
    try {
      sendHTTP(output, 500, JSON.stringify({ error: e.toString() }));
    } catch (_) {}
  }
}

function startServer() {
  if (!isEnabled()) {
    log("Disabled via preference.");
    return;
  }

  var port = getPort();

  try {
    serverSocket = Cc["@mozilla.org/network/server-socket;1"]
      .createInstance(Ci.nsIServerSocket);
    serverSocket.init(port, true, -1);

    serverSocket.asyncListen({
      onSocketAccepted: function(socket, transport) {
        var input = transport.openInputStream(0, 0, 0);
        var output = transport.openOutputStream(0, 0, 0);
        var asyncInput = input.QueryInterface(Ci.nsIAsyncInputStream);

        asyncInput.asyncWait({
          onInputStreamReady: function(stream) {
            try {
              var sis = Cc["@mozilla.org/scriptableinputstream;1"]
                .createInstance(Ci.nsIScriptableInputStream);
              sis.init(stream);
              var avail = sis.available();
              var data = avail > 0 ? sis.read(avail) : "";
              sis.close();
              handleRequest(data, output);
            } catch (e) {
              log("Read error: " + e);
              try { output.close(); } catch (_) {}
            }
          }
        }, 0, 0, Services.tm.mainThread);
      },

      onStopListening: function(socket, status) {
        log("Server stopped (status=" + status + ")");
      }
    });

    log("Listening on 127.0.0.1:" + port);
  } catch (e) {
    log("Failed to start: " + e);
    serverSocket = null;
  }
}

function stopServer() {
  if (serverSocket) {
    try {
      serverSocket.close();
      log("Server closed.");
    } catch (e) {
      log("Error closing: " + e);
    }
    serverSocket = null;
  }
}

// --- Zotero 7/8 bootstrap lifecycle ---

function startup(data, reason) {
  log("Starting v" + data.version + " (reason=" + reason + ")");
  Zotero.uiReadyPromise.then(function() { startServer(); });
}

function shutdown(data, reason) {
  log("Shutting down (reason=" + reason + ")");
  stopServer();
}

function install(data, reason) {
  log("Installed v" + data.version);
}

function uninstall(data, reason) {
  log("Uninstalled v" + data.version);
}
