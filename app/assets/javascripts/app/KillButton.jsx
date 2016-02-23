define(['react', 'socket'], function (React, Socket) {

    return React.createClass({

        killPriceServer: function () {
            Socket.send("k");
        },

        render: function () {
            return (
                <div className="tile white">
                    <div className="center-block">
                        <form className="form-inline">
                            <button type="button" className="btn btn-danger" onClick={this.killPriceServer}>Kill Pricer</button>
                        </form>
                    </div>
                </div>
            );
        }

    });

});