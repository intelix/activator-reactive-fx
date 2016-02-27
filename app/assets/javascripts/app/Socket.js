define(['appEvents'], function (Events) {

    var socket;

    var reconnectTimer = false;
    var reconnectInterval = 100;
    var connectionTimeout = 2000;
    var connected = false;

    var q = window.location.search;
    var idx = q.indexOf('?port=');
    var queryString = idx > -1 ? q.substring(idx + 6) : false;
    var websocketEndpoint = "ws://localhost:" + (queryString ? queryString : "8080");

    function connect() {

        console.info("Connecting to " + websocketEndpoint);

        socket = new WebSocket(websocketEndpoint);

        var timeout = setTimeout(function () {
            if (socket) socket.close();
        }, connectionTimeout);

        socket.onopen = function (x) {
            clearTimeout(timeout);
            Events.WebsocketConnected.dispatch();
            connected = true;
            console.info("Websocket connected");
        };
        socket.onclose = function (x) {
            clearTimeout(timeout);
            connected = false;
            console.info("Websocket disconnected");
            reconnectTimer = setTimeout(function () {
                reconnectTimer = false;
                connect();
            }, reconnectInterval);
        };
        socket.onmessage = function (e) {
            var payload = e.data;
            parsePayload(payload);
        };
    }

    function parsePayload(p) {
        var segments = p.split(":");
        if (segments[0] == "p") send("o:" + segments[1]);
        if (segments[0] == "u") Events.PriceUpdateReceived.dispatch({
            pairId: parseInt(segments[1]),
            price: parseInt(segments[2]),
            source: parseInt(segments[3])
        });
    }

    function send(p) {
        socket.send(p);
    }

    function isConnected() {
        return connected;
    }

    connect();

    return {
        send: send,
        isConnected: isConnected
    };

});