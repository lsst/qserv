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

    // This is a typical patern - complement each application with
    // the application-speciic CSS. The CSS is supposed to cover artifacts
    // within the application's container.
    CSSLoader.load('webfwk/css/FwkTestApp.css');

    class FwkTestApp extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */ 
        static update_ival_sec() { return 1; }

        constructor(name) {
            super(name);
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {

            // Note that the application name (this.fwk_app_name) comes from the base class
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
                if (now_sec - this._prev_update_sec > FwkTestApp.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
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
<p>This is a placeholder for application <span class="fwk-test-app-name">`+this.fwk_app_name+`</span></p>`;
            this.fwk_app_container.html(html);
        }

        /**
         * Nothing really gets loaded here. The method demoes an option for periodic
         * updates of the application's area.
         */
        _load() {
            console.log('load: ' + this.fwk_app_name);
        }
    }

    // Export the new class to clients via RequireJS
    return FwkTestApp;
});

