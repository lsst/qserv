define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'qserv/Common',
    'underscore',
    'modules/sql-formatter.min'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         Common,
         _,
         sqlFormatter) {

    CSSLoader.load('qserv/css/StatusQueryInspector.css');

    class StatusQueryInspector extends FwkApplication {

        /// @returns the suggested server-side timeout for retreiving results 
        static update_ival_sec() { return 3600; }

        constructor(name) {
            super(name);
            this._queryId = 0;
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
              this._init();
              if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > StatusQueryInspector.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._load();
                }
            }
        }

        /// Set the identifier and begin loading the query info in the background.
        set_query_id(queryId) {
            this._init();
            this._set_query_id(queryId);
            this._load();
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
<div class="form-row" id="fwk-status-query-controls">
  <div class="form-group col-md-1">
    <label for="query-id">Query Id:</label>
    <input type="number" id="queryId" class="form-control" value="">
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
  <div class="col" id="fwk-status-query-status">
    <div id="status"></div>
  </div>
</div>
<div class="row" id="fwk-status-query-info">
  <div class="col col-md-2">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">Status</th>
          <td style="text-align:left" id="status"><pre></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Chunks</th>
          <td style="text-align:left"><pre id="chunkCount"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Result Rows</th>
          <td style="text-align:left"><pre id="resultRows"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Result Bytes</th>
          <td style="text-align:left"><pre id="resultBytes"></pre></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">Type</th>
          <td style="text-align:left"><pre id="qType"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Submitted</th>
          <td style="text-align:left"><pre id="submitted"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Completed</th>
          <td style="text-align:left"><pre id="completed"></pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Returned</th>
          <td style="text-align:left"><pre id="returned"></pre></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-7">
    <textarea style="" disabled id="query"></textarea>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-status-query-messages">
      <thead class="thead-light">
        <tr>
          <th class="left-aligned"><elem style="color:red;">&darr;</elem>&nbsp;Time</th>
          <th class="right-aligned">Chunk</th>
          <th class="left-aligned">Source</th>
          <th class="left-aligned">Severity</th>
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
                this._status_obj = this.fwk_app_container.find('div#fwk-status-query-status > div#status');
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
        _get_query_id() { return this._form_control('input', 'queryId').val(); }
        _set_query_id(queryId) { this._form_control('input', 'queryId').val(queryId); }
        _query_info(attr) {
            if (this._query_info_obj === undefined) {
                this._query_info_obj = this.fwk_app_container.find('div#fwk-status-query-info');
            }
            return this._query_info_obj.find("#" + attr);
        }
        _set_query_info_state(attr, val) {
            this._query_info(attr).html(val);
        }
        _set_query_info(attr, val) {
            this._query_info(attr).text(val);
        }
        _table_messages() {
            if (this._table_messages_obj === undefined) {
                this._table_messages_obj = this.fwk_app_container.find('table#fwk-status-query-messages');
            }
            return this._table_messages_obj;
        }
        _load() {
            if (!this._get_query_id()) {
                this._status().html('<span style="color:maroon">Quey Id is not set</span>');
            }
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            this._load_query_info(this._get_query_id());
        }
        _load_query_info(queryId) {
            Fwk.web_service_GET(
                "/replication/qserv/master/query/" + queryId,
                {include_messages: 1, version: Common.RestAPIVersion},
                (data) => {
                    console.log(data["queries_past"]);
                    if (!data.success) {
                        _on_failed(data.error);
                        return;
                    }
                    if (!_.has(data, "queries_past") || data["queries_past"].length !== 1) {
                        _on_failed('No info returned by the server for queryId=' + queryId);
                        return;
                    } else {
                        this._display(data["queries_past"][0]);
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failed('No Response'); }
            );
        }
        _on_failed(msg) {
            this._set_query_info_state("status", "");
            this._set_query_info("qType", "");
            this._set_query_info("chunkCount", "");
            this._set_query_info("resultRows", "");
            this._set_query_info("resultBytes", "");
            this._set_query_info("submitted", "");
            this._set_query_info("completed", "");
            this._set_query_info("returned",  "");
            this._set_query_info("query", "");
            this._status().html('<span style="color:maroon">' + msg + '</span>');
            this._status().removeClass('updating');
            this._loading = false;
        }
        _display(info) {
            let sqlFormatterConfig = {"language":"mysql","uppercase:":true,"indent":"  "};
            this._set_query_info_state("status", `<pre class="${this._status2class(info.status)}">${info.status}</pre>`);
            this._set_query_info("qType", info.qType);
            this._set_query_info("chunkCount", info.chunkCount);
            this._set_query_info("resultRows", info.resultRows);
            this._set_query_info("resultBytes", info.resultBytes);
            this._set_query_info("submitted", info.submitted ? (new Date(info.submitted)).toLocalTimeString('iso') : "");
            this._set_query_info("completed", info.completed ? (new Date(info.completed)).toLocalTimeString('iso') : "");
            this._set_query_info("returned", info.returned ? (new Date(info.returned)).toLocalTimeString('iso') : "");
            this._set_query_info("query", info.query.length < 80 ? info.query : sqlFormatter.format(info.query, sqlFormatterConfig));
            let html = '';
            for (let i in info.messages) {
                let msg = info.messages[i];
                html += `
<tr>
  <td class="left-aligned"><pre>${msg.timestamp ? (new Date(msg.timestamp)).toLocalTimeString('iso') : ""}</pre></td>
  <td class="right-aligned"><pre>${msg.chunkId}</pre></td>
  <td class="left-aligned"><pre>${msg.msgSource}</pre></td>
  <td class="left-aligned"><pre class="${this._severity2class(msg.severity)}">${msg.severity}</pre></td>
  <td class="right-aligned"><pre>${msg.code}</pre></td>
  <td class="left-aligned" id="${i}">Loading...</td>
</tr>`;
            }
            let tbody = this._table_messages().children('tbody');
            tbody.html(html);
            // Messages are set via DOM to avoid failures that may arrise during static HTML generation
            // due to special HTML tags a=ro symbols like '<', '>', etc.
            for (let i in info.messages) {
                tbody.find('td#' + i).text(info.messages[i].message);
            }
        }
        _status2class(status) {
            if (status === "EXECUTING") return "query-in-progress";
            else if (status === "COMPLETED") return "query-completed";
            return "query-failed";
        }
        _severity2class(severity) {
          if (severity === "INFO") return "severity-info";
          else if (severity === "WARN") return "severity-warn";
          return "severity-error";
      }
    }
    return StatusQueryInspector;
});