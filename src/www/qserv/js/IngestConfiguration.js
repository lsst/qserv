define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'qserv/Common',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         Common,
         _) {

    CSSLoader.load('qserv/css/IngestConfiguration.css');

    class IngestConfiguration extends FwkApplication {

        static _config_params = [
            'SSL_VERIFYHOST',
            'SSL_VERIFYPEER',
            'CAPATH', 'CAINFO',
            'CAINFO_VAL',
            'PROXY_SSL_VERIFYHOST',
            'PROXY_SSL_VERIFYPEER',
            'PROXY_CAPATH',
            'PROXY_CAINFO',
            'PROXY_CAINFO_VAL',
            'CURLOPT_PROXY',
            'CURLOPT_NOPROXY',
            'CURLOPT_HTTPPROXYTUNNEL',
            'CONNECTTIMEOUT',
            'TIMEOUT',
            'LOW_SPEED_LIMIT',
            'LOW_SPEED_TIME',
            'ASYNC_PROC_LIMIT'];

        constructor(name) {
            super(name);
            // The collections of names/identifiers will be passed to the dependent pages
            // when selecting actions on transactions.
            this._databases = [];
        }

        /// @see FwkApplication.fwk_app_on_show
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /// @see FwkApplication.fwk_app_on_hide
        fwk_app_on_hide() {
        }

        /// @see FwkApplication.fwk_app_on_update
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                this._init();
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > this._update_interval_sec()) {
                    this._prev_update_sec = now_sec;
                    this._load();
                }
            }
        }

        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            this._prevTimestamp = 0;
            let html = `
<div class="form-row" id="fwk-ingest-configuration-controls">
  <div class="form-group col-md-1">
    <label for="database-status">Status:</label>
    <select id="database-status" class="form-control form-control-view">
      <option value=""></option>
      <option value="INGESTING" selected>INGESTING</option>
      <option value="PUBLISHED">PUBLISHED</option>
    </select>
  </div>
  <div class="form-group col-md-3">
    <label for="database">Database:</label>
    <select id="database" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-1">
    ${Common.html_update_ival('update-interval')}
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-ingest-configuration">
      <caption class="updating">Loading...</caption>
      <thead class="thead-light">
        <tr>
          <th class="sticky">parameter</th>
          <th class="sticky">value</th>
        </tr>
      </thead>
      <tbody>`;
            for (let i in IngestConfiguration._config_params) {
                let param = IngestConfiguration._config_params[i];
                html += `
        <tr>
          <th style="text-align:left" scope="row">${param}</th>
          <td style="text-align:left"><pre id="${param}"></pre></td>
        </tr>`;
            }
            html += `
      </tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find("#update-interval").change(() => {
                this._load();
            });
            cont.find(".form-control-view").change(() => {
                this._load();
            });
            this._disable_selectors(true);
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _get_database_status() { return this._form_control('select', 'database-status').val(); }
        _get_database() { return this._form_control('select', 'database').val(); }
        _set_database(val) { this._form_control('select', 'database').val(val); }
        _set_databases(databases) {
            // Keep the current selection after updating the selector in case if the
            // database belongs to this collection.
            const current_database = this._get_database();
            let in_collection = false;
            this._form_control('select', 'database').html(
                _.reduce(
                    databases,
                    (html, name) => {
                        if (name === current_database) in_collection = true;
                        const selected = !html ? 'selected' : ''; 
                        return html + `<option value="${name}" ${selected}>${name}</option>`;
                    },
                    ''
                )
            );
            if (in_collection && current_database) this._set_database(current_database);
        }
        _disable_selectors(disable) {
            this.fwk_app_container.find(".form-control-view").prop('disabled', disable);
        }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }
        _table() {
            if (_.isUndefined(this._table_obj)) {
                this._table_obj = this.fwk_app_container.find('table#fwk-ingest-configuration');
            }
            return this._table_obj;
        }
        _set_param(param, val) {
            if (_.isUndefined(this._config_params_obj)) this._config_params_obj = {};
            if (!_.has(this._config_params_obj, param)) {
                this._config_params_obj[param] = this._table().find('#' + param);
            }
            this._config_params_obj[param].text(val);
        }
        _status() {
            if (_.isUndefined(this._status_obj)) {
                this._status_obj = this._table().children('caption');
            }
            return this._status_obj;
        }
        _load() {
            if (_.isUndefined(this._loading)) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            this._disable_selectors(true);
            this._load_databases(this._get_database_status());
        }
        _load_databases(status) {
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._databases = _.map(
                        _.filter(
                            data.config.databases,
                            function (info) {
                                return (status === "") ||
                                       ((status === "PUBLISHED") && info.is_published) ||
                                       ((status === "INGESTING") && !info.is_published);
                            }
                        ),
                        function (info) { return info.database; }
                    );
                    this._set_databases(this._databases);
                    this._load_configuration();
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_configuration() {
            const current_database = this._get_database();
            if (!current_database) {
                this._on_failure("No databases found in this status category");
                return;
            }
            Fwk.web_service_GET(
                "/ingest/config",
                {database: current_database, version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._display(data["config"]);
                    this._disable_selectors(false);
                    Fwk.setLastUpdate(this._status());
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _on_failure(msg) {
            this._status().html(`<span style="color:maroon">${msg}</span>`);
            for (let i in IngestConfiguration._config_params) {
                let param = IngestConfiguration._config_params[i];
                this._set_param(param, '');
            }
            this._disable_selectors(false);
            this._status().removeClass('updating');
            this._loading = false;
        }
        _display(config) {
            for (let i in IngestConfiguration._config_params) {
                let param = IngestConfiguration._config_params[i];
                this._set_param(param, _.has(config, param) ? config[param] : '');
            }
        }
    }
    return IngestConfiguration;
});
