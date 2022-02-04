define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/ReplicationConfigWorkers.css');

    class ReplicationConfigWorkers extends FwkApplication {

        /**
         * @returns the default update interval for the page.
         */ 
        static update_ival_sec() { return 10; }

        constructor(name) {
            super(name);
        }

        /**
         * Override event handler defined in the base class.
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class.
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {}

        /**
         * Override event handler defined in the base class.
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > ReplicationConfigWorkers.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
                }
            }
        }

        /**
         * The first time initialization of the page's layout.
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
    <table class="table table-sm table-hover" id="fwk-controller-config-workers">
      <thead class="thead-light">
        <tr>
          <th class="sticky">name</th>
          <th class="sticky">enabled</th>
          <th class="sticky">read-only</th>
          <th class="sticky">Repl svc</th>
          <th class="sticky">File svc</th>
          <th class="sticky">Ingest svc</th>
          <th class="sticky">Exporter svc</th>
          <th class="sticky">HTTP Ingest svc</th>
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
         * Table for displaying Configuration parameters of the workers.
         * @returns JQuery table object
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-controller-config-workers');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web servie then render it to the application's page.
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
         * Display the configuration.
         */
        _display(data) {
            var config = data.config;
            let html = '';
            for (let i in config.workers) {
                let worker = config.workers[i];
                let workerEnabledCssClass  = worker.is_enabled   ? '' : 'class="table-warning"';
                let workerReadOnlyCssClass = worker.is_read_only ? 'class="table-warning"' : '';
                html += `
<tr>
  <th style="text-align:left" scope="row"><pre>` + worker.name + `</pre></th>
  <td ` + workerEnabledCssClass  + `><pre>` + (worker.is_enabled ? 'yes' : 'no') + `</pre></td>
  <td ` + workerReadOnlyCssClass + `><pre>` + (worker.is_read_only ? 'yes' : 'no') + `</pre></td>
  <td><pre>` + worker.svc_host + `:` + worker.svc_port + `</pre></td>
  <td><pre>` + worker.fs_host + `:` + worker.fs_port + `</pre></td>
  <td><pre>` + worker.loader_host + `:` + worker.loader_port + `</pre></td>
  <td><pre>` + worker.exporter_host + `:` + worker.exporter_port + `</pre></td>
  <td><pre>` + worker.http_loader_host + `:` + worker.http_loader_port + `</pre></td>
</tr>`;
            }
            this._table().children('tbody').html(html);
        }
    }
    return ReplicationConfigWorkers;
});

