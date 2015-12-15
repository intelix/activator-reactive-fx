define(['signals'], function(Signal) {

    return {
        WebsocketConnected: new Signal(),
        NewTileAdded: new Signal(),
        PriceUpdateReceived: new Signal()
    };

});