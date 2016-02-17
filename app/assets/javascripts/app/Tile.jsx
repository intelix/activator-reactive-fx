define(['react', 'socket', 'appEvents'], function (React, Socket, Events) {

    return React.createClass({

        getInitialState: function () {
            return {price: "N/A", source: ""}
        },

        componentWillMount: function () {
            Socket.send("r:" + this.props.pairId);
            Events.PriceUpdateReceived.add(this.onUpdate);
        },

        componentWillUnmount: function() {
            Events.PriceUpdateReceived.remove(this.onUpdate);
        },

        onUpdate: function (u) {
            if (u.pairId == this.props.pairId) {
                var price = _.padRight(u.price / 100000, 7, '0');
                var source = "Server " + u.source;
                this.setState({price: price, source: source});
            }
        },

        render: function () {
            var state = this.state;
            return (
                <div className="tile blue">
                    <div className="price">{state.price}</div>
                    <div className="ccyPair">{this.props.pair}</div>
                    <div className="source">{state.source}</div>
                </div>
            );
        }

    });

});