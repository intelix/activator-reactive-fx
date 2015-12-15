define(['react', 'app/RequestQuoteForm', 'app/Tiles', 'app/KillButton'], function (React, RequestQuoteForm, Tiles, KillButton) {

    return React.createClass({

        render: function () {
            return (
                <div className="container">
                    <div className="row">
                        <div className="col-md-12">
                            <h1><strong>Reactive FX</strong></h1>
                        </div>
                    </div>
                    <div className="row">
                        <div className="col-sm-4"><RequestQuoteForm /></div>
                        <div className="col-sm-3"></div>
                        <div className="col-sm-2"><KillButton /></div>
                    </div>
                    <div className="row"><Tiles /></div>
                </div>
            );
        }

    });

});