define(['react', 'appEvents', 'constants'], function(React, Events, Constants) {

    return React.createClass({

        handleSubmit: function() {
            var cId = this.refs.cId.value;
            var pair = Constants.currencies[cId];
            Events.NewTileAdded.dispatch({id: cId, pair: pair});
        },

        render: function () {
            return (
                <div className="tile green">
                    <div className="center-block">
                        <form className="form-inline">
                            <select className="form-control" ref="cId">
                                {Constants.currencies.map(function(next,i) {
                                    return <option value={i} key={i}>{next}</option>;
                                })}
                            </select>
                            <button type="button" className="btn btn-default space" onClick={this.handleSubmit}>Request Quote</button>
                        </form>
                    </div>
                </div>
            );
        }

    });

});