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

    CSSLoader.load('qserv/css/QservWorkerMySQLQueries.css');

    class QservWorkerMySQLQueries extends FwkApplication {

        constructor(name) {
            super(name);
            this._mySqlThreadId2Expanded = {};  // Store 'true' to allow persistent state for the expanded
                                                // queries between updates.
            this._mySqlThreaId2query = {};      // Store query text for each identifier. The dictionary gets
                                                // updated at each refresh of the page.
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
        set_worker(worker) {
            this._init();
            this._load(worker);
        }
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="row" id="fwk-worker-mysql-queries-controls">
  <div class="col">
    <div class="form-row">

      <div class="form-group col-md-2">
        <label for="num-queries"># displayed / total:</label>
        <input type="text" id="num-queries" class="form-control" value="0 / 0" disabled>
      </div>
      <div class="form-group col-md-3">
        <label for="worker">Worker:</label>
        <select id="worker" class="form-control form-control-selector">
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="query-command">Command:</label>
        <select id="query-command" class="form-control form-control-selector">
          <option value="">&lt;any&gt;</option>
          <option value="Query" selected>Query</option>
          <option value="Sleep">Sleep</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 10)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-controls-form">&nbsp;</label>
        <button id="reset-controls-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-worker-mysql-queries">
      <thead class="thead-light">
        <tr>
          <th style="text-align:right;">Task</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th style="text-align:right;">MySQL</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
          <th>&nbsp;</th>
        </tr>
        <tr>
          <th class="sticky" style="text-align:right;">QID</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky" style="text-align:right;">job</th>
          <th class="sticky" style="text-align:right;">chunk</th>
          <th class="sticky" style="text-align:right;">subchunk</th>
          <th class="sticky" style="text-align:right;">templ</th>
          <th class="sticky" style="text-align:right;">state</th>
          <th class="sticky" style="text-align:right;">Id</th>
          <th class="sticky" style="text-align:right;">Time</th>
          <th class="sticky" style="text-align:right;">Command</th>
          <th class="sticky" style="text-align:right;">State</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-clipboard-fill"></i></th>
          <th class="sticky">Query</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-selector").change(() => {
                this._load();
            });
            cont.find("button#reset-controls-form").click(() => {
                this._set_update_interval_sec(10);
                this._set_query_command('Query');
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
        _set_num_queries(total, displayed) { this._form_control('input', 'num-queries').val(displayed + ' / ' + total); }
        _query_command() { return this._form_control('select', 'query-command').val(); }
        _set_query_command(val) { this._form_control('select', 'query-command').val(val); }
        _worker() { return this._form_control('select', 'worker').val(); }
        _set_worker(val) { this._form_control('select', 'worker').val(val); }
        _set_workers(workers) {
            const prev_worker = this._worker();
            let html = '';
            for (let i in workers) {
                const worker = workers[i];
                const selected = (_.isEmpty(prev_worker) && (i === 0)) ||
                                 (!_.isEmpty(prev_worker) && (prev_worker === worker));
                html += `
 <option value="${worker}" ${selected ? "selected" : ""}>${worker}</option>`;
            }
            this._form_control('select', 'worker').html(html);
        }
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-worker-mysql-queries');
            }
            return this._table_obj;
        }
        _load(worker = undefined) {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    let workers = [];
                    for (let i in data.config.workers) {
                        workers.push(data.config.workers[i].name);
                    }
                    this._set_workers(workers);
                    if (!_.isUndefined(worker)) this._set_worker(worker);
                    this._load_queries();
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _load_queries() {
            Fwk.web_service_GET(
                "/replication/qserv/worker/db/" + this._worker(),
                {   timeout_sec: 2, version: Common.RestAPIVersion
                },
                (data) => {
                    if (data.success) {
                        this._display(data.status);
                        Fwk.setLastUpdate(this._table().children('caption'));
                    } else {
                        console.log('request failed', this.fwk_app_name, data.error);
                        this._table().children('caption').html('<span style="color:maroon">' + data.error + '</span>');
                    }
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _display(status) {
            const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";
            const queryCopyTitle = "Click to copy the query text to the clipboard.";
            const COL_Id = 0, COL_Command = 4, COL_Time = 5, COL_State = 6, COL_Info = 7;
            const desiredQueryCommand = this._query_command();
            let tbody = this._table().children('tbody');
            if (_.isEmpty(status.queries.columns)) {
                tbody.html('');
                return;
            }
            this._mySqlThreaId2query = {};
            let numQueriesTotal = 0;
            let numQueriesDisplayed = 0;
            let html = '';
            for (let i in status.queries.rows) {
                numQueriesTotal++;
                // MySQL query context
                let row = status.queries.rows[i];
                const thisQueryCommand = row[COL_Command];
                if ((desiredQueryCommand !== '') && (thisQueryCommand !== desiredQueryCommand)) continue;
                let mySqlThreadId = row[COL_Id];
                let query = row[COL_Info];
                this._mySqlThreaId2query[mySqlThreadId] = query;
                const expanded = (mySqlThreadId in this._mySqlThreadId2Expanded) && this._mySqlThreadId2Expanded[mySqlThreadId];
                const queryToggleTitle = "Click to toggle query formatting.";
                const queryStyle = "color:#4d4dff;";
                // Task context (if any)
                let queryId = '';
                let jobId = '';
                let chunkId = '';
                let subChunkId = '';
                let templateId = '';
                let state = '';
                if (_.has(status.mysql_thread_to_task, mySqlThreadId)) {
                    let task = status.mysql_thread_to_task[mySqlThreadId];
                    queryId    = task['query_id'];
                    jobId      = task['job_id'];
                    chunkId    = task['chunk_id'];
                    subChunkId = task['subchunk_id'];
                    templateId = task['template_id'];
                    state      = task['state'];
                }
                const rowClass = QservWorkerMySQLQueries._state2css(state);
                html += `
<tr mysql_thread_id="${mySqlThreadId}" query_id="${queryId}">
  <th style="text-align:right;"><pre>${queryId}</pre></th>`;
                if (queryId === '') {
                    html += `
  <td style="text-align:center; padding-top:0; padding-bottom:0">&nbsp;</td>`;
                } else {
                    html += `
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>`;
                }
                html += `
  <td style="text-align:right;" class="${rowClass}"><pre>${jobId}</pre></td>
  <td style="text-align:right;" class="${rowClass}"><pre>${chunkId}</pre></td>
  <td style="text-align:right;" class="${rowClass}"><pre>${subChunkId}</pre></td>
  <td style="text-align:right;" class="${rowClass}"><pre>${templateId}</pre></td>
  <td style="text-align:right;" class="${rowClass}"><pre>${state}</pre></td>
  <th style="text-align:right;"><pre>${mySqlThreadId}</pre></th>
  <td style="text-align:right;"><pre>${row[COL_Time]}</pre></td>
  <td style="text-align:right;"><pre>${row[COL_Command]}</pre></td>
  <td style="text-align:right;"><pre>${row[COL_State]}</pre></td>`;
                if (query === '') {
                    html += `
  <td style="text-align:center; padding-top:0; padding-bottom:0">&nbsp;</td>
  <td>&nbsp;</td>`;

                } else {
                    html += `
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-dark btn-sm copy-query" style="height:20px; margin:0px;" title="${queryCopyTitle}"></button>
  </td>
  <td class="query_toggler" title="${queryToggleTitle}"><pre class="query" style="${queryStyle}">` + this._query2text(mySqlThreadId, expanded) + `<pre></td>`;
                }
                html += `
</tr>`;
                numQueriesDisplayed++;
            }
            tbody.html(html);
            let that = this;
            let displayQuery  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("query_id");
                Fwk.find("Status", "Query Inspector").set_query_id(queryId);
                Fwk.show("Status", "Query Inspector");
            };
            let copyQueryToClipboard = function(e) {
                let button = $(e.currentTarget);
                let mySqlThreadId = button.parent().parent().attr("mysql_thread_id");
                let query = that._mySqlThreaId2query[mySqlThreadId];
                navigator.clipboard.writeText(query,
                    () => {},
                    () => { alert("Failed to write the query to the clipboard. Please copy the text manually: " + query); }
                );
            };
            let toggleQueryDisplay = function(e) {
                let td = $(e.currentTarget);
                let pre = td.find("pre.query");
                const mySqlThreadId = td.parent().attr("mysql_thread_id");
                const expanded = !((mySqlThreadId in that._mySqlThreadId2Expanded) && that._mySqlThreadId2Expanded[mySqlThreadId]);
                pre.text(that._query2text(mySqlThreadId, expanded));
                that._mySqlThreadId2Expanded[mySqlThreadId] = expanded;
            };
            tbody.find("button.inspect-query").click(displayQuery);
            tbody.find("button.copy-query").click(copyQueryToClipboard);
            tbody.find("td.query_toggler").click(toggleQueryDisplay);
            this._set_num_queries(numQueriesTotal, numQueriesDisplayed);
        }
        _query2text(mySqlThreadId, expanded) {
            return Common.query2text(this._mySqlThreaId2query[mySqlThreadId], expanded);
        }
        static _state2css(state) {
            switch (state) {
                case 'CREATED': return 'table-warning';
                case 'QUEUED': return 'table-light';
                case 'STARTED': return 'table-danger';
                case 'EXECUTING_QUERY': return 'table-primary';
                case 'READING_DATA': return 'table-info';
                case 'FINISHED': return 'table-secondary';
                default: return '';
            }
        }
    }
    return QservWorkerMySQLQueries;
});
