define({
    load: function (url) {
        var link = document.createElement("link");
        link.type = "text/css";
        link.rel = "stylesheet";
        link.href = url + "?bust="+new Date().getTime();
        document.getElementsByTagName("head")[0].appendChild(link);
    }
});
