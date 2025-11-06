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

    CSSLoader.load('qserv/css/IngestUserTableRequest.css');

    class IngestUserTableRequest extends FwkApplication {

        /// Return the default number of the last seconds to track in the request history
        static last_seconds() { return 15 * 60; }

        constructor(name) {
            super(name);
            this._data = undefined;
            this._queries_chart = undefined;
        }
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                this._init();
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > this._update_interval_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
                }
            }
        }
        /// Set an identifier of the request and begin loading info in the background.
        set_id(request_id) {
            this._init();
            this._set_request_id(request_id);
            this._load();
        }
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="form-row" id="fwk-ingest-user-table-request-controls">
  <div class="form-group col-md-1">
    <label for="request-id">Request Id:</label>
    <input type="number" id="request-id" class="form-control" value="">
  </div>
  <div class="form-group col-md-1">
    ${Common.html_update_ival('update-interval', 10)}
  </div>
</div>
<div class="row">
  <div class="col" id="fwk-ingest-user-table-request-status">
    <div id="status"></div>
  </div>
</div>
<div class="row" id="fwk-ingest-user-table-request-general">
  <div class="col col-md-1 header">
    GENERAL
  </div>
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">status</th>
          <td style="text-align:left" id="status"></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">begin_time</th>
          <td style="text-align:left"><pre id="begin_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">end_time</th>
          <td style="text-align:left"><pre id="end_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">delete_time</th>
          <td style="text-align:left"><pre id="delete_time"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">&nbsp;</th>
          <td style="text-align:left">&nbsp;</td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-5">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">database</th>
          <td style="text-align:left" id="database"><pre></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">table</th>
          <td style="text-align:left"><pre id="table"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">table_type</th>
          <td style="text-align:left"><pre id="table_type"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">is_temporary</th>
          <td style="text-align:left"><pre id="is_temporary"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">data_format</th>
          <td style="text-align:left"><pre id="data_format"></pre></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">transaction_id</th>
          <td style="text-align:left"><pre id="transaction_id"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">num_chunks</th>
          <td style="text-align:left"><pre id="num_chunks"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">num_rows</th>
          <td style="text-align:left"><pre id="num_rows"></pre></td>
         </tr>
        <tr>
          <th style="text-align:left" scope="row">num_bytes</th>
          <td style="text-align:left"><pre id="num_bytes"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">&nbsp;</th>
          <td style="text-align:left">&nbsp;</td>
        </tr>
      </tbody>
    </table>
  </div>

</div>
<div class="row" id="fwk-ingest-user-table-request-extended">
  <div class="col col-md-1 header">
    EXTENDED
  </div>
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <tbody>
      </tbody>
    </table>
  </div>
</div>
<div class="row" id="fwk-ingest-user-table-request-errors">
  <div class="col col-md-1 header">
    ERROR(S)
  </div>
  <div class="col col-md-11" id="error">
  </div>
</div>
<div class="row" id="fwk-ingest-user-table-request-schema">
  <div class="col col-md-1 header">
    SCHEMA
  </div>
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <thead class="thead-light">
        <tr>
          <th>Column</th>
          <th>Type</th>
        </tr>
      </thead>
      <tbody>
      </tbody>
    </table>
  </div>
</div>
<div class="row" id="fwk-ingest-user-table-request-indexes">
  <div class="col col-md-1 header">
    INDEXES
  </div>
  <div class="col col-md-11">
    <table class="table table-sm table-hover">
      <thead class="thead-light">
        <tr>
          <th>Name</th>
          <th>Spec</th>
          <th>Columns</th>
          <th>Comment</th>
        </tr>
      </thead>
      <tbody>
      </tbody>
    </table>
  </div>

</div>
`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control").change(() => {
                this._load();
            });
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this.fwk_app_container.find('div#fwk-ingest-user-table-request-status > div#status');
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
        _get_request_id() { return this._form_control('input', 'request-id').val(); }
        _set_request_id(request_id) { this._form_control('input', 'request-id').val(request_id); }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }
        _set_update_interval_sec(val) { this._form_control('select', 'update-interval').val(val); }
        _general() {
            if (this._general_obj === undefined) {
                this._general_obj = this.fwk_app_container.find('div#fwk-ingest-user-table-request-general');
            }
            return this._general_obj;
        }
        _general_attr(attr) {
            return this._general().find("#" + attr);
        }
        _set_general_html(attr, val) {
            this._general_attr(attr).html(val);
        }
        _set_general(attr, val) {
            this._general_attr(attr).text(val);
        }
        _set_errors(val) {
            if (this._errors_obj === undefined) {
                this._errors_obj = this.fwk_app_container.find('div#fwk-ingest-user-table-request-errors > #error');
            }

            // If val is a JSON string, pretty print it. Otherwise just show it as is.
            // Truncate if too long.
            const maxLen = 2048;
            try {
                val = JSON.stringify(JSON.parse(val), null, 2);
                let len = val ? val.length : 0;
                if (len > maxLen) {
                    val = val.substr(0, maxLen) + ' ...';
                }
                this._errors_obj.html('<pre>' + val + '</pre>');
            } catch (e) {
                let len = val ? val.length : 0;
                if (len > maxLen) {
                    val = val.substr(0, maxLen) + ' ...';
                }
                this._errors_obj.text(val ? val.substr(0, maxLen) : '');
            }
        }
        _schema() {
            if (this._schema_obj === undefined) {
                this._schema_obj = this.fwk_app_container.find('div#fwk-ingest-user-table-request-schema table > tbody');
            }
            return this._schema_obj;
        }
        _indexes() {
            if (this._indexes_obj === undefined) {
                this._indexes_obj = this.fwk_app_container.find('div#fwk-ingest-user-table-request-indexes table > tbody');
            }
            return this._indexes_obj;
        }
        _extended() {
            if (this._extended_obj === undefined) {
                this._extended_obj = this.fwk_app_container.find('div#fwk-ingest-user-table-request-extended table > tbody');
            }
            return this._extended_obj;
        }
        _load() {
            if (!this._get_request_id()) {
                this._status().html('<span style="color:maroon">Request id is not set</span>');
            }
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            Fwk.web_service_GET(
                "/replication/qserv/master/ingest-requests",
                {   id: this._get_request_id(),
                    extended: 1,
                    version: Common.RestAPIVersion
                },
                (data) => {
                    if (!data.success) {
                        this._on_failed(data.error);
                        return;
                    }
                    if (!_.has(data, "requests") || data["requests"].length != 1) {
                        this._on_failed('No info returned by the server for request_id=' + this._get_request_id());
                        return;
                    } else {
                        this._display(data["requests"][0]);
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failed(msg); }
            );
        }
        _on_failed(msg) {
            this._status().html('<span style="color:maroon">' + msg + '</span>');
            this._status().removeClass('updating');
            this._loading = false;
        }
        _display(req) {
            console.log(req);
            this._set_general_html("status", `<pre class="${IngestUserTableRequest._request_status_class(req.status)}">${req.status}</pre>`);
            this._set_general("begin_time", new Date(req.begin_time).toLocalTimeString('iso'));
            this._set_general("end_time", req.end_time ? new Date(req.end_time).toLocalTimeString('iso') : '');
            this._set_general("delete_time", req.delete_time ? new Date(req.delete_time).toLocalTimeString('iso') : '');
            this._set_general("transaction_id", req.transaction_id);
            this._set_general("num_chunks", req.num_chunks);
            this._set_general("num_rows", req.num_rows);
            this._set_general("num_bytes", req.num_bytes);
            this._set_general("database", req.database);
            this._set_general("table", req.table);
            this._set_general("table_type", req.table_type);
            this._set_general("is_temporary", req.is_temporary);
            this._set_general("data_format", req.data_format);
            this._set_errors(req.error);
            let html ='';
            if (req.schema) {
                for (let i in req.schema) {
                    let col = req.schema[i];
                    html += `<tr><td>${col.name}</td><td>${col.type}</td></tr>`;
                }
            }
            this._schema().html(html);
            html = '';
            if (req.indexes) {
                for (let i in req.indexes) {
                    let idx = req.indexes[i];
                    html +=
`<tr>
  <td>${idx.index}</td>
  <td>${idx.spec}</td>
  <td>${JSON.stringify(idx.columns, null, 2)}</td>
  <td>${idx.comment}</td>
</tr>`;
                }
            }
            this._indexes().html(html);
            html = '';
            if (req.extended) {
                for (let key in req.extended) {
                    html +=
`<tr>
  <td style="text-align:left; font-weight: bold;">${key}</td>
  <td style="text-align:left;"><pre>${req.extended[key]}</pre></td>
</tr>`;
                }
            }
            this._extended().html(html);
        }
        static _request_status_class(status) {
            switch (status) {
                case 'IN_PROGRESS': return 'status-in-progress';
                case 'FAILED':      return 'status-failed';
                case 'FAILED_LR':   return 'status-failed-lr';
            }
            return '';
        }
    }
    return IngestUserTableRequest;
});
