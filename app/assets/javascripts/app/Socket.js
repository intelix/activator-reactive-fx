define(['appEvents'], function (Events) {

    var socket;

    var reconnectTimer = false;
    var reconnectInterval = 100;
    var connectionEstablishTimeout = 2000;

    function _attemptConnection() {
        console.info("!>>> Trying to connect....");

        socket = new WebSocket("ws://localhost:8080");
        var timeout = setTimeout(function () {
            if (socket) socket.close();
        }, connectionEstablishTimeout);

        socket.onopen = function (x) {
            clearTimeout(timeout);
            console.info("!>> Websocket connected");
        };
        socket.onclose = function (x) {
            clearTimeout(timeout);
            console.info("!>> Websocket disconnected");
            reconnectTimer = setTimeout(function () {
                reconnectTimer = false;
                _attemptConnection();
            }, reconnectInterval);
        };
        socket.onmessage = function (e) {
            var payload = e.data;
            _parse(payload);
        };
    }

    _attemptConnection();

    function _parse(p) {
        var segments = p.split(":");
        if (segments[0] == "p") {
            _send("o:" + segments[1]);
            console.info("!>>>> Ping!");
        }
        if (segments[0] == "u") {
            Events.PriceUpdateReceived.dispatch({
                pairId: parseInt(segments[1]),
                price: parseInt(segments[2])
            });
        }
    }

    function _send(p) {
        socket.send(p);
    }

    return {
        send: _send
    };

});