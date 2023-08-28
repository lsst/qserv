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

    CSSLoader.load('qserv/css/StatusPastQueries.css');

    class StatusPastQueries extends FwkApplication {

        /// @returns the suggested server-side timeout for retreiving results 
        static _server_proc_timeout_sec() { return 2; }

        constructor(name) {
            super(name);
            this._queryId2Expanded = {};  // Store 'true' to allow persistent state for the expanded
                                          // queries between updates.
            this._id2query = {};          // Store query text for each identifier
        }

        /**
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /**
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }

        /**
         * @see FwkApplication.fwk_app_on_update
         */
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
<div class="row" id="fwk-status-past-queries-controls">
  <div class="col">
    <h3>Search queries</h3>
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="query-age">Submitted:</label>
        <select id="query-age" class="form-control form-control-selector">
          <option selected value="0">MOST RECENTLY</option>
          <option value="300">5 MINUTES AGO</option>
          <option value="900">15 MINUTES AGO</option>
          <option value="1800">30 MINUTES AGO</option>
          <option value="3600">1 HOUR AGO</option>
          <option value="7200">2 HOURS AGO</option>
          <option value="14400">4 HOURS AGO</option>
          <option value="28800">8 HOURS AGO</option>
          <option value="43200">12 HOURS AGO</option>
          <option value="86400">1 DAY AGO</option>
          <option value="172800">2 DAYS AGO</option>
          <option value="604800">1 WEEK AGO</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="query-status">Status:</label>
        <select id="query-status" class="form-control form-control-selector">
          <option value="" selected></option>
          <option value="COMPLETED">COMPLETED</option>
          <option value="FAILED">FAILED</option>
          <option value="ABORTED">ABORTED</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="min-elapsed">Min.elapsed [sec]:</label>
        <input type="number" id="min-elapsed" class="form-control form-control-selector" value="0">
      </div>
      <div class="form-group col-md-1">
        <label for="query-type">Type:</label>
        <select id="query-type" class="form-control form-control-selector">
          <option value="" selected></option>
          <option value="SYNC">SYNC</option>
          <option value="ASYNC">ASYNC</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="query-search-pattern">Search pattern:</label>
        <input type="text" id="query-search-pattern" class="form-control form-control-selector" value="">
      </div>
      <div class="form-group col-md-1">
        <label for="query-search-mode">Search mode:</label>
        <select id="query-search-mode" class="form-control form-control-selector">
          <option value="LIKE" selected>LIKE</option>
          <option value="REGEXP">REGEXP</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="max-queries">Max.queries:</label>
        <select id="max-queries" class="form-control form-control-selector">
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
        ${Common.html_update_ival('update-interval', 5)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-queries-form">&nbsp;</label>
        <button id="reset-queries-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
    <table class="table table-sm table-hover" id="fwk-status-past-queries">
      <caption class="updating">
        Loading...
      </caption>
      <thead class="thead-light">
        <tr>
          <th class="sticky">Submitted</th>
          <th class="sticky">Status</th>
          <th class="sticky" style="text-align:right;">Elapsed</th>
          <th class="sticky">Type</th>
          <th class="sticky" style="text-align:right;">Chunks</th>
          <th class="sticky" style="text-align:right;">Ch/min</th>
          <th class="sticky" style="text-align:right;">&sum;&nbsp;Bytes</th>
          <th class="sticky" style="text-align:right;">&sum;&nbsp;Rows</th>
          <th class="sticky" style="text-align:right;">Rows</th>
          <th class="sticky" style="text-align:right;">QID</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-clipboard-fill"></i></th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky">Query</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-selector").change(() => {
                this._load();
            });
            cont.find("button#reset-queries-form").click(() => {
                this._set_query_age("0");
                this._set_query_status("");
                this._set_min_elapsed("0");
                this._set_query_type("");
                this._set_query_search_pattern("");
                this._set_query_search_mode("LIKE");
                this._set_max_queries("200");
                this._load();
            });
        }
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-status-past-queries');
            }
            return this._table_obj;
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this._table().children('caption');
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
        _get_query_age()       { return this._form_control('select', 'query-age').val(); }
        _set_query_age(val)    { this._form_control('select', 'query-age').val(val); }

        _get_query_status()    { return this._form_control('select', 'query-status').val(); }
        _set_query_status(val) { this._form_control('select', 'query-status').val(val); }

        _get_min_elapsed()     { return this._form_control('input', 'min-elapsed').val(); }
        _set_min_elapsed(val)  { this._form_control('input', 'min-elapsed').val(val); }

        _get_query_type()      { return this._form_control('select', 'query-type').val(); }
        _set_query_type(val)   { this._form_control('select', 'query-type').val(val); }

        _get_query_search_pattern()    { return this._form_control('input', 'query-search-pattern').val(); }
        _set_query_search_pattern(val) { this._form_control('input', 'query-search-pattern').val(val); }

        _get_query_search_mode()    { return this._form_control('select', 'query-search-mode').val(); }
        _set_query_search_mode(val) { this._form_control('select', 'query-search-mode').val(val); }

        _get_max_queries()     { return this._form_control('select', 'max-queries').val(); }
        _set_max_queries(val)  { this._form_control('select', 'max-queries').val(val); }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }

        _load() {
            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._status().addClass('updating');

            Fwk.web_service_GET(
                "/replication/qserv/master/queries/past",
                {   version: Common.RestAPIVersion,
                    query_age: this._get_query_age(),
                    query_status: this._get_query_status(),
                    min_elapsed_sec: this._get_min_elapsed(),
                    query_type: this._get_query_type(),
                    search_pattern: this._get_query_search_pattern(),
                    search_regexp_mode: this._get_query_search_mode() == "REGEXP" ? 1 : 0,
                    limit4past: this._get_max_queries()
                },
                (data) => {
                    if (!data.success) {
                        this._status().html(`<span style="color:maroon">${data.error}</span>`);
                    } else {
                        this._display(data);
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _display(data) {
            this._id2query = {};
            const queryToggleTitle = "Click to toggle query formatting.";
            const queryCopyTitle = "Click to copy the query text to the clipboard.";
            const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";
            const queryStyle = "color:#4d4dff;";
            let html = '';
            let that = this;
            let toggleQueryDisplay = function(e) {
                let td = $(e.currentTarget);
                let pre = td.find("pre.query");
                const queryId = td.parent().attr("id");
                const expanded = !((queryId in that._queryId2Expanded) && that._queryId2Expanded[queryId]);
                pre.text(that._query2text(queryId, expanded));
                that._queryId2Expanded[queryId] = expanded;
            };
            let copyQueryToClipboard = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                let query = that._id2query[queryId];
                navigator.clipboard.writeText(query,
                    () => {},
                    () => { alert("Failed to write the query to the clipboard. Please copy the text manually: " + query); }
                );
            };
            let displayQuery  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                Fwk.find("Status", "Query Inspector").set_query_id(queryId);
                Fwk.show("Status", "Query Inspector");
            };
            for (let i in data.queries_past) {
                let query = data.queries_past[i];
                this._id2query[query.queryId] = query.query;
                let elapsed = this._elapsed(query.completed_sec - query.submitted_sec);
                let failed_query_class = query.status !== "COMPLETED" ? "table-danger" : "";
                let performance = this._performance(query.chunkCount, query.completed_sec - query.submitted_sec);
                let expanded = (query.queryId in this._queryId2Expanded) && this._queryId2Expanded[query.queryId];
                html += `
<tr class="${failed_query_class}" id="${query.queryId}">
  <td style="padding-right:10px;"><pre>` + query.submitted + `</pre></td>
  <td style="padding-right:10px;"><pre>${query.status}</pre></td>
  <th style="text-align:right; padding-top:0;">${elapsed}</th>
  <td><pre>` + query.qType + `</pre></td>
  <th style="text-align:right;"><pre>${query.chunkCount}</pre></th>
  <td style="text-align:right;" ><pre>${performance > 0 ? performance : ''}</pre></td>
  <th style="text-align:right;"><pre>${query.collectedBytes}</pre></th>
  <th style="text-align:right;"><pre>${query.collectedRows}</pre></th>
  <th style="text-align:right;"><pre>${query.finalRows}</pre></th>
  <th style="text-align:right;"><pre>${query.queryId}</pre></th>
  <td style="text-align:right; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-dark btn-sm copy-query" style="height:20px; margin:0px;" title="${queryCopyTitle}"></button>
  </td>
  <td style="text-align:right; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
  <td class="query_toggler" title="${queryToggleTitle}"><pre class="query" style="${queryStyle}">` + this._query2text(query.queryId, expanded) + `</pre></td>
</tr>`;
            }
            let tbodyPastQueries = this._table().children('tbody').html(html);
            tbodyPastQueries.find("td.query_toggler").click(toggleQueryDisplay);
            tbodyPastQueries.find("button.copy-query").click(copyQueryToClipboard);
            tbodyPastQueries.find("button.inspect-query").click(displayQuery);
        }
        
        /**
         * @param {Number} seconds
         * @returns {String} the amount of time elapsed by a query, formatted as: 'hh:mm:ss'
         */
        _elapsed(totalSeconds) {
            if (_.isUndefined(totalSeconds)) return '<span>&nbsp;</span>';
            let hours   = Math.floor(totalSeconds / 3600);
            let minutes = Math.floor((totalSeconds - 3600 * hours) / 60);
            let seconds = (totalSeconds - 3600 * hours - 60 * minutes) % 60;
            let displayHours   = hours !== 0;
            let displayMinutes = displayHours || minutes !== 0;
            let displaySeconds = true;
            return '<span>' +
                   (displayHours   ? (hours   < 10 ? '0' : '') + hours   + 'h' : '') + ' ' +
                   (displayMinutes ? (minutes < 10 ? '0' : '') + minutes + 'm' : '') + ' ' +
                   (displaySeconds ? (seconds < 10 ? '0' : '') + seconds + 's' : '') +
                   '</span>';
        }

        /**
         * @param {integer} chunks
         * @param {integer} totalSeconds
         * @returns {integer} the number of chunks per minute (or 0 if the totalSeconds is 0)
         */
        _performance(chunks, totalSeconds) {
            if (chunks === 0 || totalSeconds <= 0) return 0;
            return Math.floor(chunks / (totalSeconds / 60.));
        }
        _query2text(queryId, expanded) {
            return Common.query2text(this._id2query[queryId], expanded);
        }
    }
    return StatusPastQueries;
});
