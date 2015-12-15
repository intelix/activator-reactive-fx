define(['react', 'appEvents', 'socket'], function (React, Events, Socket) {

    var currencies = [
        "AUD/CAD", "AUD/CHF", "AUD/NZD",
        "AUD/USD", "CAD/CHF", "EUR/GBP",
        "EUR/CHF", "EUR/USD", "GBP/AUD",
        "GBP/CAD", "GBP/CHF", "GBP/USD",
        "USD/CAD", "USD/CHF", "NZD/USD"
    ];

    return React.createClass({

        getInitialState: function () {
            return {connected: Socket.isConnected()}
        },

        onConnected: function () {
            this.setState({connected: true});
        },

        handleSubmit: function () {
            var cId = this.refs.cId.value;
            var pair = currencies[cId];
            Events.NewTileAdded.dispatch({id: cId, pair: pair});
        },

        componentWillMount: function () {
            Events.WebsocketConnected.add(this.onConnected);
        },

        render: function () {

            var button;

            if (this.state.connected)
                button = <button type="button" className="btn btn-default space" onClick={this.handleSubmit}>Request Quote</button>;
            else
                button = <button type="button" className="btn btn-default space disabled" onClick={this.handleSubmit}>Connecting...</button>;

            return (
                <div className="tile green">
                    <div className="center-block">
                        <form className="form-inline">
                            <select className="form-control" ref="cId">
                                {currencies.map(function (next, i) {
                                    return <option value={i} key={i}>{next}</option>;
                                })}
                            </select>
                            {button}
                        </form>
                    </div>
                </div>
            );
        }

    });

});