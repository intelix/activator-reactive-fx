define(['signals'], function(Signal) {

    return {
        NewTileAdded: new Signal(),
        PriceUpdateReceived: new Signal()
    };

});