requirejs.config({
    paths: {
        jquery: "/assets/lib/jquery/jquery.min",
        react: "/assets/lib/react/react-with-addons",
        lodash: "/assets/lib/lodash/lodash.min",
        signals: "/assets/lib/js-signals/signals.min",

        socket: "app/Socket",
        constants: "app/Constants",
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

require(["socket", "app/main"]);