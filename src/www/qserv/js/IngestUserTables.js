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

    CSSLoader.load('qserv/css/IngestUserTables.css');

    class IngestUserTables extends FwkApplication {

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
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            const lastMinutes = [15, 30, 45, 60];
            const lastHours = [2, 4, 8, 12, 16, 20, 24];
            const lastDays = [2, 3, 4, 5, 6, 7];
            let html = `
<div class="row" id="fwk-ingest-user-tables-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="request-status">Status:</label>
        <select id="request-status" class="form-control form-control-selector">
          <option value="" selected>&lt;any&gt;</option>
          <option value="IN_PROGRESS">IN_PROGRESS</option>
          <option value="COMPLETED">COMPLETED</option>
          <option value="FAILED">FAILED</option>
          <option value="FAILED_LR">FAILED_LR</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="table-type">Type:</label>
        <select id="table-type" class="form-control form-control-selector">
          <option value="" selected>&lt;any&gt;</option>
          <option value="FULLY_REPLICATED">FULLY_REPLICATED</option>
          <option value="DIRECTOR">DIRECTOR</option>
          <option value="CHILD">CHILD</option>
          <option value="REF_MATCH">REF_MATCH</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="table-deleted">Deleted:</label>
        <select id="table-deleted" class="form-control form-control-selector">
          <option value="0" selected>&lt;any&gt;</option>
          <option value="1">Yes</option>
          <option value="-1">No</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="database-search-pattern">Database search pattern:</label>
        <input type="text" id="database-search-pattern" class="form-control form-control-selector" value="">
      </div>
      <div class="form-group col-md-1">
        <label for="database-search-mode">Search mode:</label>
        <select id="database-search-mode" class="form-control form-control-selector">
          <option value="LIKE" selected>LIKE</option>
          <option value="REGEXP">REGEXP</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="min-num-chunks">Min.chunks:</label>
        <input type="number" id="min-num-chunks" class="form-control form-control-selector" value="0">
      </div>
      <div class="form-group col-md-1">
        <label for="min-num-bytes">Min.bytes:</label>
        <input type="number" id="min-num-bytes" class="form-control form-control-selector" value="0">
      </div>
      <div class="form-group col-md-1">
        <label for="min-num-rows">Min.rows:</label>
        <input type="number" id="min-num-rows" class="form-control form-control-selector" value="0">
      </div>
      <div class="form-group col-md-1">
        <label for="max-requests">Max.requests:</label>
        <select id="max-requests" class="form-control form-control-selector">
          <option value="10">10</option>
          <option value="50">50</option>
          <option value="100">100</option>
          <option value="200" selected>200</option>
          <option value="300">300</option>
          <option value="400">400</option>
          <option value="500">500</option>
          <option value="600">600</option>
          <option value="700">700</option>
          <option value="800">800</option>
          <option value="900">900</option>
          <option value="1000">1,000</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 10)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-form">&nbsp;</label>
        <button id="reset-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-ingest-user-tables">
      <thead class="thead-light">
        <tr>
          <th class="sticky center-aligned"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky center-aligned"><i class="bi bi-bar-chart-steps"></i></th>
          <th class="sticky right-aligned">Q</th>
          <th class="sticky right-aligned">Id</th>
          <th class="sticky right-aligned"><elem style="color:red;">&darr;</elem></th>
          <th class="sticky right-aligned">Started</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky right-aligned">Finished</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky right-aligned">Deleted</th>
          <th class="sticky right-aligned">Status</th>
          <th class="sticky right-aligned">Database</th>
          <th class="sticky right-aligned">Table</th>
          <th class="sticky right-aligned">Type</th>
          <th class="sticky right-aligned">Temporary</th>
          <th class="sticky right-aligned">Format</th>
          <th class="sticky right-aligned">Chunks</th>
          <th class="sticky right-aligned">Rows</th>
          <th class="sticky right-aligned">Bytes</th>
        </tr>
      </thead>
      <caption>Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-selector").change(() => {
                this._load();
            });
            cont.find("button#reset-form").click(() => {
                this._set_update_interval_sec(10);
                this._set_request_status('');
                this._set_table_type('');
                this._set_table_deleted("0");
                this._set_database_search_pattern('');
                this._set_database_search_mode('LIKE');
                this._set_min_num_chunks("0");
                this._set_min_num_bytes("0");
                this._set_min_num_rows("0");
                this._set_max_requests("200");
                this._disable_selectors();
                this._load();
            });
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }
        _set_update_interval_sec(val) { this._form_control('select', 'update-interval').val(val); }
        _request_status() { return this._form_control('select', 'request-status').val(); }
        _set_request_status(val) { this._form_control('select', 'request-status').val(val); }
        _table_type() { return this._form_control('select', 'table-type').val(); }
        _set_table_type(val) { this._form_control('select', 'table-type').val(val); }
        _table_deleted() { return this._form_control('select', 'table-deleted').val(); }
        _set_table_deleted(val) { this._form_control('select', 'table-deleted').val(val); }
        _database_search_pattern() { return this._form_control('input', 'database-search-pattern').val(); }
        _set_database_search_pattern(val) { this._form_control('input', 'database-search-pattern').val(val); }
        _database_search_mode() { return this._form_control('select', 'database-search-mode').val(); }
        _set_database_search_mode(val) { this._form_control('select', 'database-search-mode').val(val); }
        _get_min_num_chunks() { return this._form_control('input', 'min-num-chunks').val(); }
        _set_min_num_chunks(val) { this._form_control('input', 'min-num-chunks').val(val); }
        _get_min_num_bytes() { return this._form_control('input', 'min-num-bytes').val(); }
        _set_min_num_bytes(val) { this._form_control('input', 'min-num-bytes').val(val); }
        _get_min_num_rows() { return this._form_control('input', 'min-num-rows').val(); }
        _set_min_num_rows(val) { this._form_control('input', 'min-num-rows').val(val); }
        _get_max_requests()     { return this._form_control('select', 'max-requests').val(); }
        _set_max_requests(val)  { this._form_control('select', 'max-requests').val(val); }
        _table() {
            if (_.isUndefined(this._table_obj)) {
                this._table_obj = this.fwk_app_container.find('table#fwk-ingest-user-tables');
            }
            return this._table_obj;
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this._table().children('caption');
            }
            return this._status_obj;
        }
        _disable_selectors(disabled=true) {
            this._form_control('select', 'request-status').prop('disabled', disabled);
            this._form_control('select', 'table-type').prop('disabled', disabled);
            this._form_control('select', 'table-deleted').prop('disabled', disabled);
            this._form_control('input', 'database-search-pattern').prop('disabled', disabled);
            this._form_control('select', 'database-search-mode').prop('disabled', disabled);
            this._form_control('input', 'min-num-chunks').prop('disabled', disabled);
            this._form_control('input', 'min-num-bytes').prop('disabled', disabled);
            this._form_control('input', 'min-num-rows').prop('disabled', disabled);
        }
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._table().children('caption').addClass('updating');
            this._disable_selectors(true);
            Fwk.web_service_GET(
                "/replication/qserv/master/ingest-requests",
                {   timeout_sec: 2,
                    version: Common.RestAPIVersion,
                    status: this._request_status(),
                    type: this._table_type(),
                    deleted: this._table_deleted(),
                    database_search_pattern: this._database_search_pattern(),
                    database_search_regexp_mode: this._database_search_mode() === 'REGEXP' ? 1 : 0,
                    min_num_chunks: this._get_min_num_chunks(),
                    min_num_bytes: this._get_min_num_bytes(),
                    min_num_rows: this._get_min_num_rows(),
                    limit: this._get_max_requests()
                },
                (data) => {
                    if (data.success) {
                        this._display(data.requests);
                        Fwk.setLastUpdate(this._status());
                    } else {
                        console.log('request failed', this.fwk_app_name, data.error);
                        this._status().html('<span style="color:maroon">' + data.error + '</span>');
                    }
                    this._loading = false;
                    this._disable_selectors(false);
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._loading = false;
                    this._disable_selectors(false);
                }
            );
        }
        _display(requests) {
            const requestInspectTitle = 'Click to see extended parameters of the request';
            const contribInspectTitle = 'Click to see contributions made in a scope of the transaction';
            const searchQservQueriesTitle = 'Click to see the Qserv queries which were referring this database and table';
            let html = '';
            for (let i in requests) {
                let req = requests[i];
                let statusCssClass = '';
                switch (req.status) {
                    case 'IN_PROGRESS': statusCssClass = 'alert alert-success'; break;
                    case 'FAILED':      statusCssClass = 'alert alert-danger'; break;
                    case 'FAILED_LR':   statusCssClass = 'alert alert-warning'; break;
                }
                const beginDateTimeStr = (new Date(req.begin_time)).toLocalTimeString('iso').split(' ');
                const beginDateStr = beginDateTimeStr[0];
                const beginTimeStr = beginDateTimeStr[1];
                const endTimeStr  = req.end_time === 0 ? '' : (new Date(req.end_time)).toLocalTimeString('iso').split(' ')[1];
                const endDeltaStr = req.begin_time && req.end_time ? ((req.end_time - req.begin_time) / 1000).toFixed(1) : '';
                const deleteDateTimeStr =  req.delete_time === 0 ? '' : (new Date(req.delete_time)).toLocalTimeString('iso');
                const deleteDeltaStr = req.end_time && req.delete_time ? ((req.delete_time - req.end_time) / 1000).toFixed(1) : '';
                const isTemporaryStr = req.is_temporary ? 'yes' : 'no';
                html += `
<tr class="${statusCssClass}">
  <th class="controls" style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm activity" id="${req.id}" style="height:20px; margin:0px;" title="${requestInspectTitle}"></button>
  </th>`;
                if (req.transaction_id === 0) {
                    html += `
  <th style="text-align:center; padding-top:0; padding-bottom:0">&nbsp;</th>`;
                } else {
                    html += `
  <th class="controls" style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm contrib" transaction_id="${req.transaction_id}" database="${req.database}" style="height:20px; margin:0px;" title="${contribInspectTitle}"></button>
  </th>`;
                }
                html += `
  <th class="controls" style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm queries" style="height:20px; margin:0px;" database="${req.database}" table="${req.table}" title="${searchQservQueriesTitle}"></button>
  </th>
  <th class="right-aligned"><pre>${req.id}</pre></th>
  <th class="right-aligned"><pre>${beginDateStr}</pre></th>
  <td class="right-aligned"><pre>${beginTimeStr}</pre></td>
  <th class="right-aligned"><pre>${endDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${endTimeStr}</pre></td>
  <th class="right-aligned"><pre>${deleteDeltaStr}</pre></th>
  <th class="right-aligned"><pre>${deleteDateTimeStr}</pre></th>
  <td class="right-aligned"><pre>${req.status}</pre></td>
  <th class="right-aligned"><pre>${req.database}</pre></th>
  <td class="right-aligned"><pre class="database_table" database="${req.database}" table="${req.table}">${req.table}</pre></td>
  <td class="right-aligned"><pre>${req.table_type}</pre></td>
  <td class="right-aligned"><pre>${isTemporaryStr}</pre></td>
  <td class="right-aligned"><pre>${req.data_format}</pre></td>
  <td class="right-aligned"><pre>${req.num_chunks}</pre></td>
  <td class="right-aligned"><pre>${req.num_rows}</pre></td>
  <td class="right-aligned"><pre>${Common.format_data_rate(req.num_bytes)}</pre></td>
</tr>`;
            }
            let tbody = this._table().children('tbody');
            tbody.html(html);
            tbody.find("button.activity").click(
                (e) => {
                    const id = $(e.currentTarget).attr("id");
                    Fwk.find("Ingest", "User Table Request").set_id(id);
                    Fwk.show("Ingest", "User Table Request");
                }
            );
            tbody.find("button.contrib").click(
                (e) => {
                    const worker = undefined;
                    const database = $(e.currentTarget).attr("database");
                    const table = undefined;
                    const transactionId = $(e.currentTarget).attr("transaction_id");
                    const status = '';  // Any status
                    Fwk.find("Ingest", "Contributions").search(worker, database, table, transactionId, status);
                    Fwk.show("Ingest", "Contributions");
                }
            );
            tbody.find("button.queries").click(
                (e) => {
                    const database = $(e.currentTarget).attr("database");
                    const table = $(e.currentTarget).attr("table");
                    Fwk.find("Status", "Past Queries").search_queries_by(database, table);
                    Fwk.show("Status", "Past Queries");
                }
            );
            tbody.find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
        }
    }
    return IngestUserTables;
});
