define([
    'webfwk/Class',
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(Class,
         CSSLoader,
         Fwk,
         FwkApplication) {

    CSSLoader.load('webfwk/css/FwkApplicationControlApp.css');

    class FwkApplicationControlApp extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */ 
        static update_ival_sec() { return 10; }

        constructor(name) {
            super(name);
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > FwkApplicationControlApp.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                }
            }
        }

        /**
         * The first time initialization of the page's layout
         */
        _init() {
            if (this._initialized === undefined) {
                this._initialized = false;
            }
            if (this._initialized) return;
            this._initialized = true;

            // Note that the application's container (this.fwk_app_container) object (JQuery) comes
            // from the base class
            let html = `
<p>Buttons shown on this page demonstrate the Framework's capability of
  switching between applications.
<p>`;
            this._appPaths = Fwk.appPaths();
            for (let i in this._appPaths) {
                let path = this._appPaths[i];
                let label = path[0] + (_.isUndefined(path[1]) ? '' : '&nbsp;/&nbsp;' + path[1]);
                html += `
<div id="fwk-uitests-appcontrol">
  <button type="button" class="btn btn-secondary btn-sm" id="`+i+`">`+label+`</button>
</div>`;
            }
            this.fwk_app_container.html(html);
            this.fwk_app_container.find('button').on('click', (e) => {
                console.log(e);
                console.log(e.currentTarget);
                let i = $(e.currentTarget).attr('id');
                let path = this._appPaths[i];
                Fwk.show(path[0], path[1]);
            });
        }
    }
    return FwkApplicationControlApp;
});

