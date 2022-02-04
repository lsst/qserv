define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/ReplicationConfigGeneral.css');

    class ReplicationConfigGeneral extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */ 
        static update_ival_sec() { return 10; }

        constructor(name) {
            super(name);
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {}

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > ReplicationConfigGeneral.update_ival_sec()) {
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

            let html = `
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-controller-config-general">
      <thead class="thead-light">
        <tr>
          <th class="sticky">parameter</th>
          <th class="sticky">value</th>
          <th class="sticky">description</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            this.fwk_app_container.html(html);
        }

        /**
         * Table for displaying the general Configuration parameters
         * @returns JQuery table object
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-controller-config-general');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web servie then render it to the application's
         * page.
         */
        _load() {
            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/config",
                {},
                (data) => {
                    this._display(data);
                    Fwk.setLastUpdate(this._table().children('caption'));
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }

        /**
         * Display the configuration
         */
        _display(data) {
            var config = data.config;
            let html = '';
            for (const category in config.general) {
                for (const param in config.general[category]) {
                    const value = config.general[category][param];
                    html += `
<tr>
  <th style="text-align:left" scope="row"><pre>` + category + '.' + param + `</pre></th>
  <td><pre>` + value + `</pre></td>
  <td>` + config.meta[category][param].description + `</td>
</tr>`;
                }
            }
            this._table().children('tbody').html(html);
        }
    }
    return ReplicationConfigGeneral;
});

