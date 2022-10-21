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

    CSSLoader.load('qserv/css/IngestContribWarnings.css');

    class IngestContribWarnings extends FwkApplication {

        /// @returns the suggested server-side timeout for retreiving results 
        static update_ival_sec() { return 3600; }

        constructor(name) {
            super(name);
        }

        /// @see FwkApplication.fwk_app_on_show
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /// @see FwkApplication.fwk_app_on_hide
        fwk_app_on_hide() {}

        /// @see FwkApplication.fwk_app_on_update
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                this._init();
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > IngestContribWarnings.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._load();
                }
            }
        }

        /// Set an identifier of a  contribution and begin loading info in the background.
        set_contrib_id(contrib_id) {
            this._init();
            this._set_contrib_id(contrib_id);
            this._load();
        }

        /// The first time initialization of the page's layout
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="form-row" id="fwk-ingest-contrib-warnings-controls">
  <div class="form-group col-md-1">
    <label for="contrib-id">Contribution Id:</label>
    <input type="number" id="contrib-id" class="form-control" value="">
  </div>
  <div class="form-group col-md-1">
    <label for="query-update-interval">Interval <i class="bi bi-arrow-repeat"></i></label>
    <select id="query-update-interval" class="form-control">
    <option value="10">10 sec</option>
    <option value="20">20 sec</option>
    <option value="30" selected>30 sec</option>
    <option value="60">1 min</option>
    <option value="120">2 min</option>
    <option value="300">5 min</option>
    </select>
  </div>
</div>
<div class="row">
  <div class="col" id="fwk-ingest-contrib-warnings-status">
    <div id="status"></div>
  </div>
</div>
<div class="row" id="fwk-ingest-contrib-warnings-info">
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">status</th>
          <td style="text-align:left" id="status"></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">create_time</th>
          <td style="text-align:left"><pre id="create_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">start_time</th>
          <td style="text-align:left"><pre id="start_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">read_time</th>
          <td style="text-align:left"><pre id="read_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">load_time</th>
          <td style="text-align:left"><pre id="load_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">num_bytes</th>
          <td style="text-align:left"><pre id="num_bytes"></pre></td>
         </tr>
        <tr>
          <th style="text-align:left" scope="row">num_rows</th>
          <td style="text-align:left"><pre id="num_rows"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">num_rows_loaded</th>
          <td style="text-align:left"><pre id="num_rows_loaded"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">fields_terminated_by</th>
          <td style="text-align:left"><pre id="fields_terminated_by"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">fields_enclosed_by</th>
          <td style="text-align:left"><pre id="fields_enclosed_by"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">fields_escaped_by</th>
          <td style="text-align:left"><pre id="fields_escaped_by"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">lines_terminated_by</th>
          <td style="text-align:left"><pre id="lines_terminated_by"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">max_num_warnings</th>
          <td style="text-align:left"><pre id="max_num_warnings"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">num_warnings</th>
          <td style="text-align:left"><pre id="num_warnings"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">http_error</th>
          <td style="text-align:left"><pre id="http_error"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">system_error</th>
          <td style="text-align:left"><pre id="system_error"></pre></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-9">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">tmp_file</th>
          <td style="text-align:left"><pre id="tmp_file"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">transaction_id</th>
          <td style="text-align:left" id="transaction_id"><pre></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">worker</th>
          <td style="text-align:left"><pre id="worker"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">database</th>
          <td style="text-align:left"><pre id="database"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">table</th>
          <td style="text-align:left"><pre id="table"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">chunk</th>
          <td style="text-align:left"><pre id="chunk"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">overlap</th>
          <td style="text-align:left"><pre id="overlap"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">url</th>
          <td style="text-align:left"><pre id="url"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">async</th>
          <td style="text-align:left"><pre id="async"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">http_method</th>
          <td style="text-align:left"><pre id="http_method"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">http_data</th>
          <td style="text-align:left"><pre id="http_data"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">http_headers</th>
          <td style="text-align:left"><pre id="http_headers"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">error</th>
          <td style="text-align:left"><pre id="error"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">retry_allowed</th>
          <td style="text-align:left"><pre id="retry_allowed"></pre></td>
        </tr>
      </tbody>
    </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-ingest-contrib-warnings-table">
      <thead class="thead-light">
        <tr>
          <th class="left-aligned">Level</th>
          <th class="right-aligned">Code</th>
          <th class="left-aligned">Message</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control").change(() => {
                this._load();
            });
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this.fwk_app_container.find('div#fwk-ingest-contrib-warnings-status > div#status');
            }
            return this._status_obj;
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _get_contrib_id() { return this._form_control('input', 'contrib-id').val(); }
        _set_contrib_id(contrib_id) { this._form_control('input', 'contrib-id').val(contrib_id); }
        _table_warnings() {
            if (this._table_warnings_obj === undefined) {
                this._table_warnings_obj = this.fwk_app_container.find('table#fwk-ingest-contrib-warnings-table');
            }
            return this._table_warnings_obj;
        }
        _info() {
            if (this._info_obj === undefined) {
                this._info_obj = this.fwk_app_container.find('div#fwk-ingest-contrib-warnings-info');
            }
            return this._info_obj;
        }
        _info_attr(attr) {
            return this._info().find("#" + attr);
        }
        _set_info_html(attr, val) {
            this._info_attr(attr).html(val);
        }
        _set_info(attr, val) {
            this._info_attr(attr).text(val);
        }
        _info_attr_parent(attr) {
            return this._info_attr(attr).parent();
        }
        _load() {
            if (!this._get_contrib_id()) {
                this._status().html('<span style="color:maroon">Contribution id is not set</span>');
            }
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            this._load_contrib(this._get_contrib_id());
        }
        _load_contrib(contrib_id) {
            Fwk.web_service_GET(
                "/ingest/trans/contrib/" + contrib_id,
                {include_warnings: 1, version: Common.RestAPIVersion},
                (data) => {
                    console.log(data["contribution"]);
                    if (!data.success) {
                        this._on_failed(data.error);
                        return;
                    }
                    if (!_.has(data, "contribution")) {
                        this._on_failed('No info returned by the server for contrib_id=' + contrib_id);
                        return;
                    } else {
                        this._display(data["contribution"]);
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failed('No Response'); }
            );
        }
        _on_failed(msg) {
            this._set_info_html("status", "");
            this._set_info("create_time", "");
            this._set_info("start_time", "");
            this._set_info("read_time", "");
            this._set_info("load_time", "");
            this._set_info("num_bytes", "");
            this._set_info("num_rows", "");
            this._set_info("num_rows_loaded", "");
            this._info_attr_parent("num_rows_loaded").removeClass('table-danger');
            this._set_info("max_num_warnings", "");
            this._set_info("num_warnings", "");
            this._info_attr_parent("num_warnings").removeClass('table-danger');
            this._set_info("http_error", "");
            this._set_info("system_error", "");
            this._set_info("error", "");
            this._set_info("retry_allowed", "");
            this._set_info("tmp_file", "");
            this._set_info("transaction_id", "");
            this._set_info("worker", "");
            this._set_info("database", "");
            this._set_info("table", "");
            this._set_info("chunk", "");
            this._set_info("overlap", "");
            this._set_info("url", "");
            this._set_info("async", "");
            this._set_info("fields_terminated_by", "");
            this._set_info("fields_enclosed_by", "");
            this._set_info("fields_escaped_by", "");
            this._set_info("lines_terminated_by", "");
            this._set_info("http_method", "");
            this._set_info("http_data", "");
            this._set_info("http_headers", "");
            this._status().html('<span style="color:maroon">' + msg + '</span>');
            this._status().removeClass('updating');
            this._loading = false;
        }
        _display(contrib) {
            this._set_info_html("status", `<pre class="${this._status2class(contrib.status)}">${contrib.status}</pre>`);
            this._set_info("create_time", contrib.create_time ? (new Date(contrib.create_time)).toLocalTimeString('iso') : "");
            this._set_info("start_time", contrib.start_time ? (new Date(contrib.start_time)).toLocalTimeString('iso') : "");
            this._set_info("read_time", contrib.read_time ? (new Date(contrib.read_time)).toLocalTimeString('iso') : "");
            this._set_info("load_time", contrib.load_time ? (new Date(contrib.load_time)).toLocalTimeString('iso') : "");
            this._set_info("num_bytes", contrib.num_bytes);
            this._set_info("num_rows", contrib.num_rows);
            this._set_info("num_rows_loaded", contrib.num_rows_loaded);
            if (contrib.num_rows === contrib.num_rows_loaded) {
                this._info_attr_parent("num_rows_loaded").removeClass('table-danger');
            } else {
                this._info_attr_parent("num_rows_loaded").addClass('table-danger');
            }
            this._set_info("max_num_warnings", contrib.max_num_warnings);

            this._set_info("num_warnings", contrib.num_warnings);
            if (contrib.num_warnings === 0) {
                this._info_attr_parent("num_warnings").removeClass('table-danger');
            } else {
                this._info_attr_parent("num_warnings").addClass('table-danger');
            }
            this._set_info("http_error", contrib.http_error);
            this._set_info("system_error", contrib.system_error);
            this._set_info("error", contrib.error);
            this._set_info("retry_allowed", contrib.retry_allowed);
            this._set_info("tmp_file", contrib.tmp_file);
            this._set_info("transaction_id", contrib.transaction_id);
            this._set_info("worker", contrib.worker);
            this._set_info_html("database", `<pre class="database_table" database="${contrib.database}" table="${contrib.table}">${contrib.database}</pre>`);
            this._set_info_html("table", `<pre class="database_table" database="${contrib.database}" table="${contrib.table}">${contrib.table}</pre>`);
            this._set_info("chunk", contrib.chunk);
            this._set_info("overlap", contrib.overlap);
            this._set_info("url", contrib.url);
            this._set_info("async", contrib.async);
            this._set_info("fields_terminated_by", contrib.dialect_input.fields_terminated_by);
            this._set_info("fields_enclosed_by", contrib.dialect_input.fields_enclosed_by);
            this._set_info("fields_escaped_by", contrib.dialect_input.fields_escaped_by);
            this._set_info("lines_terminated_by", contrib.dialect_input.lines_terminated_by);
            this._set_info("http_method", contrib.http_method);
            this._set_info("http_data", contrib.http_data);
            this._set_info("http_headers", contrib.http_headers);
            this._info().find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
            let html = '';
            for (let i in contrib.warnings) {
                let warning = contrib.warnings[i];
                html += `
<tr>
  <td class="left-aligned"><pre class="${this._level2class(warning.level)}">${warning.level}</pre></td>
  <th class="right-aligned"><pre>${warning.code}</pre></th>
  <td class="left-aligned" id="${i}">Loading...</td>
</tr>`;
            }
            let tbody = this._table_warnings().children('tbody');
            tbody.html(html);
            // Messages are set via DOM to avoid failures that may arrise during
            // static HTML generation due to special HTML tags or markup symbols
            // like '<', '>', etc.
            for (let i in contrib.warnings) {
                tbody.find('td#' + i).text(contrib.warnings[i].message);
            }
        }
        _level2class(level) {
            if (level === "Note") return "level-note";
            else if (level === "Warning") return "level-warning";
            return "level-error";
        }
        _status2class(status) {
            if (status === "IN_PROGRESS") return "status-progress";
            else if (status === "FINISHED") return "status-finished";
            return "status-error";
        }
    }
    return IngestContribWarnings;
});
