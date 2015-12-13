define(['react', 'socket', 'appEvents'], function (React, Socket, Events) {

    return React.createClass({

        getInitialState: function () {
            return {price: 0}
        },

        componentWillMount: function () {
            Socket.send("r:" + this.props.pairId);
            Events.PriceUpdateReceived.add(this.onUpdate);
        },

        onUpdate: function (u) {
            console.info("!>>>> update: " + u.pairId);
            if (u.pairId == this.props.pairId) {
                console.info("!>>>> my: " + this.props.pairId);
                var price = u.price;
                var s3 = price % 10;
                var s2 = (Math.floor(price / 10)) % 100;
                var s1 = Math.floor(price / 1000) / 100;
                this.setState({price: price, s1: s1, s2: s2, s3: s3});
            }
        },

        render: function () {
            var state = this.state;
            var formattedPrice = (state.price > 0)
                ? <span>{state.s1}<span className="big">{state.s2}</span>{state.s3}</span>
                : <span className="big">N/A</span>;
            return (
                <div className="tile blue">
                    <div className="price">{formattedPrice}</div>
                    <div className="ccyPair">{this.props.pair}</div>
                    <div className="source">Server 1</div>
                </div>
            );
        }

    });

});