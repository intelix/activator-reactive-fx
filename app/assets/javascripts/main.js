requirejs.config({
    paths: {
        jquery: "/assets/lib/jquery/jquery.min",
        react: "/assets/lib/react/react-with-addons",
        lodash: "/assets/lib/lodash/lodash.min",
        signals: "/assets/lib/js-signals/signals.min",

        config: "app/Config",
        socket: "app/Socket",
        appEvents: "app/AppEvents"
    },
    packages: ["app"],

    shim: {
        jquery: {
            exports: "$"
        },
        lodash: {
            exports: "_"
        },
        appEvents: {
            deps: ["signals"]
        }
    }
});

require(["lodash", "config", "socket", "app/main"]);