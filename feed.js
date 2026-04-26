(function () {
    'use strict';

    function start() {
        if (window.feed_uk_plugin == true)
            return;

        window.feed_uk_plugin = true;

        Lampa.Listener.follow('request_before', function (e) {
            if (Lampa.Storage.get('language', '') == 'uk' && e.params.url.indexOf('/feed/all') != -1) {
                e.params.url = 'https://siaivo.isroot.in/lampa-ua-pack/feed/data.uk.json?t=' + (+new Date());
            }
        });

        Lampa.Lang.add({
            "title_in_high_quality": {
                'uk': 'У високій якості',
            }
        });
    }

    if (window.appready) start();
    else {
        Lampa.Listener.follow('app', function (e) {
            if (e.type == 'ready') {
                start();
            }
        });
    }
})();
