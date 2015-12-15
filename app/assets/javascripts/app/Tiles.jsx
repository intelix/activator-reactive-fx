define(['react', 'appEvents', 'app/Tile'], function (React, Events, Tile) {

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

        render: function () {
            return (
                <span>
                    {this.state.tiles.map(function (next, i) {
                        return <div key={i} className="col-sm-2">
                            <Tile pairId={next.id} pair={next.pair}/>
                        </div>
                    })}
                </span>
            );
        }

    });

});