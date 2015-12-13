define(['react', 'appEvents', 'app/NewTileForm', 'app/Tile', 'socket'], function (React, Events, NewTileForm, Tile, Socket) {

    return React.createClass({

        getInitialState: function () {
            return {tiles: []};
        },

        componentWillMount: function () {
            Events.NewTileAdded.add(this.onNewTile);
        },

        onNewTile: function (e) {
            var newSet = React.addons.update(this.state.tiles, {$push: [e]});
            this.setState({tiles: newSet});
        },


        killPriceServer: function() {
            Socket.send("k");
        },

        render: function () {
            return (
                <div className="container">
                    <div className="row">
                        <div className="col-md-12">
                            <h1><strong>Reactive FX</strong></h1>
                        </div>
                    </div>
                    <div className="row">
                        <div className="col-sm-4">
                            <NewTileForm />
                        </div>
                        <div className="col-sm-3">
                            <div>
                                End to end latency: <b>100.3ms</b><br/>
                                Total CPU: <b>72%</b><br/>
                                Process CPU: <b>70%</b>
                            </div>
                        </div>
                        <div className="col-sm-3">
                            <div>
                                Client tps: <b>10000 msg/s</b><br/>
                                WS Endpoint tps: <b>10000 msg/s</b><br/>
                                Datasource tps: <b>10000 msg/s</b><br/>
                            </div>
                        </div>
                        <div className="col-sm-2">
                            <div className="tile white">
                                <div className="center-block">
                                    <form className="form-inline">
                                        <button type="button" className="btn btn-danger" onClick={this.killPriceServer}>Kill Price Server</button>
                                    </form>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div className="row">
                        {this.state.tiles.map(function (next, i) {
                            return <div key={i} className="col-sm-2">
                                <Tile pairId={next.id} pair={next.pair}/>
                            </div>
                        })}
                    </div>
                </div>
            );
        }

    });

});