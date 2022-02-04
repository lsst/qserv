define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore',
    'modules/sql-formatter.min'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _,
         sqlFormatter) {

    CSSLoader.load('qserv/css/StatusUserQueries.css');

    class StatusUserQueries extends FwkApplication {

        /// @returns the suggested server-side timeout for retreiving results 
        static server_proc_timeout_sec() { return 2; }

        constructor(name) {
            super(name);
            this._queryId2Expanded = {};  // Store 'true' to allow persistent state for the expanded
                                          // queries between updates.
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

            this._scheduler2color = {
                'Snail':   '#007bff',
                'Slow':    '#17a2b8',
                'Med':     '#28a745',
                'Fast':    '#ffc107',
                'Group':   '#dc3545',
                'Loading': 'default'
            };

            let html = `
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-status-queries">
      <caption class="updating">
        Loading...
      </caption>
      <thead class="thead-light">
        <tr>
          <th>started</th>
          <th>progress</th>
          <th>sched</th>
          <th style="text-align:right;">elapsed</th>
          <th style="text-align:right;">left (est.)</th>
          <th style="text-align:right;">ch[unks]</th>
          <th style="text-align:right;">ch/min</th>
          <th style="text-align:right;">qid</th>
          <th>query</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <h3>Search past queries</h3>
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
        <label for="update-interval"><i class="bi bi-arrow-repeat"></i> interval:</label>
        <select id="update-interval" class="form-control form-control-selector">
          <option value="10">5 sec</option>
          <option value="10" selected>10 sec</option>
          <option value="20">20 sec</option>
          <option value="30">30 sec</option>
          <option value="60">1 min</option>
          <option value="120">2 min</option>
          <option value="300">5 min</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="reset-queries-form">&nbsp;</label>
        <button id="reset-queries-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
    <table class="table table-sm table-hover" id="fwk-status-queries-past">
      <thead class="thead-light">
        <tr>
          <th class="sticky">&nbsp;</th>
          <th class="sticky">submitted</th>
          <th class="sticky">status</th>
          <th class="sticky" style="text-align:right;">elapsed</th>
          <th class="sticky">type</th>
          <th class="sticky" style="text-align:right;">qid</th>
          <th class="sticky">query</th>
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
                this._set_max_queries("200");
                this._load();
            });
        }

        /**
         * Table for displaying the progress of the on-going user queries
         * @returns JQuery table object
         */
        _tableQueries() {
            if (this._tableQueries_obj === undefined) {
                this._tableQueries_obj = this.fwk_app_container.find('table#fwk-status-queries');
            }
            return this._tableQueries_obj;
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this._tableQueries().children('caption');
            }
            return this._status_obj;
        }

        /**
         * Table for displaying the completed, failed, etc. user queries
         * @returns JQuery table object
         */
        _tablePastQueries() {
            if (this._tablePastQueries_obj === undefined) {
                this._tablePastQueries_obj = this.fwk_app_container.find('table#fwk-status-queries-past');
            }
            return this._tablePastQueries_obj;
        }

        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _disable_controls(disable) {
            this._form_control('select', 'query-age').prop('disabled', disable);
            this._form_control('select', 'query-status').prop('disabled', disable);
            this._form_control('input',  'min-elapsed').prop('disabled', disable);
            this._form_control('select', 'query-type').prop('disabled', disable);
            this._form_control('select', 'max-queries').prop('disabled', disable);
            this._form_control('select', 'update-interval').prop('disabled', disable);
            this._form_control('button', 'reset-queries-form').prop('disabled', disable);
        }
        _get_query_age()       { return this._form_control('select', 'query-age').val(); }
        _set_query_age(val)    { this._form_control('select', 'query-age').val(val); }

        _get_query_status()    { return this._form_control('select', 'query-status').val(); }
        _set_query_status(val) { this._form_control('select', 'query-status').val(val); }

        _get_min_elapsed()     { return this._form_control('input', 'min-elapsed').val(); }
        _set_min_elapsed(val)  { this._form_control('input', 'min-elapsed').val(val); }

        _get_query_type()      { return this._form_control('select', 'query-type').val(); }
        _set_query_type(val)   { this._form_control('select', 'query-type').val(val); }

        _get_max_queries()     { return this._form_control('select', 'max-queries').val(); }
        _set_max_queries(val)  { this._form_control('select', 'max-queries').val(val); }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }

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

            this._status().addClass('updating');
            this._disable_controls(true);

            Fwk.web_service_GET(
                "/replication/qserv/master/query",
                {   query_age: this._get_query_age(),
                    query_status: this._get_query_status(),
                    min_elapsed_sec: this._get_min_elapsed(),
                    query_type: this._get_query_type(),
                    limit4past: this._get_max_queries(),
                    timeout_sec: StatusUserQueries.server_proc_timeout_sec()
                },
                (data) => {
                    if (!data.success) {
                        this._status().html(`<span style="color:maroon">${data.error}</span>`);
                    } else {
                        this._display(data);
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._disable_controls(false);
                    this._loading = false;
                },
                (msg) => {
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._disable_controls(false);
                    this._loading = false;
                }
            );
        }

        /**
         * Display the queries
         */
        _display(data) {
            let queryTitle = "Click to toggle query formatting.";
            let sqlFormatterConfig = {"language":"mysql","uppercase:":true,"indent":"  "};
            let queryStyle = "color:#4d4dff;";
            let html = '';
            for (let i in data.queries) {
                let query = data.queries[i];
                let progress = Math.floor(100. * query.completedChunks  / query.totalChunks);
                let scheduler = _.isUndefined(query.scheduler) ? 'Loading...' : query.scheduler.substring('Sched'.length);
                let scheduler_color = _.has(this._scheduler2color, scheduler) ?
                    this._scheduler2color[scheduler] :
                    this._scheduler2color['Loading'];

                let elapsed = this._elapsed(query.samplingTime_sec - query.queryBegin_sec);
                let leftSeconds;
                if (query.completedChunks > 0 && query.samplingTime_sec - query.queryBegin_sec > 0) {
                    leftSeconds = Math.floor(
                            (query.totalChunks - query.completedChunks) /
                            (query.completedChunks / (query.samplingTime_sec - query.queryBegin_sec))
                    );
                }
                let left = this._elapsed(leftSeconds);
                let trend = this._trend(query.queryId, leftSeconds);
                let performance = this._performance(query.completedChunks, query.samplingTime_sec - query.queryBegin_sec);
                let expanded = (query.queryId in this._queryId2Expanded) && this._queryId2Expanded[query.queryId];
                let row_class = expanded ? "row_expanded" : "row_compact";
                let query_compact_class  = expanded ? "hidden"  : "visible";
                let query_expanded_class = expanded ? "visible" : "hidden";
                html += `
<tr class="${row_class}" id="${query.queryId}" title="${queryTitle}">
  <td><pre>` + query.queryBegin + `</pre></td>
  <th scope="row">
    <div class="progress" style="height: 22px;">
      <div class="progress-bar" role="progressbar" style="width: ${progress}%" aria-valuenow="${progress}" aria-valuemin="0" aria-valuemax="100">
        ${progress}%
      </div>
    </div>
  </th>
  <td style="background-color:${scheduler_color};">${scheduler}</td>
  <th style="text-align:right; padding-top:0; padding-left:10px;">${elapsed}</th>
  <td style="text-align:right; padding-top:0; padding-left:10px;">${left}${trend}</td>
  <th scope="row" style="text-align:right;  padding-left:10px;"><pre>${query.completedChunks}/${query.totalChunks}</pre></th>
  <td style="text-align:right;" ><pre>${performance}</pre></td>
  <th scope="row" style="text-align:right;"><pre>${query.queryId}</pre></th>
  <td><pre class="query compact  ${query_compact_class}"  style="${queryStyle}">` + query.query + `</pre>
      <pre class="query expanded ${query_expanded_class}" style="${queryStyle}">` + sqlFormatter.format(query.query, sqlFormatterConfig) + `</pre>
  </td>
</tr>`;
            }
            let that = this;
            let toggleQueryDisplay = function(e) {
                let tr = $(e.currentTarget);
                let queryCompact  = tr.find("pre.query.compact");
                let queryExpanded = tr.find("pre.query.expanded");
                let queryId = tr.attr("id");
                if (tr.hasClass("row_compact")) {
                    tr.removeClass("row_compact").addClass("row_expanded");
                    queryCompact.removeClass("visible").addClass("hidden");
                    queryExpanded.removeClass("hidden").addClass("visible");
                    that._queryId2Expanded[queryId] = true;
                } else {
                    tr.removeClass("row_expanded").addClass("row_compact");
                    queryCompact.removeClass("hidden").addClass("visible");
                    queryExpanded.removeClass("visible").addClass("hidden");
                    that._queryId2Expanded[queryId] = false;
                }
            };
            this._tableQueries().children('tbody').html(html).children("tr").click(toggleQueryDisplay);
            html = '';
            for (let i in data.queries_past) {
                let query = data.queries_past[i];
console.log(query);
                let elapsed = this._elapsed(query.completed_sec - query.submitted_sec);
                let failed_query_class = query.status !== "COMPLETED" ? "table-danger" : "";
                let attention = query.qType == 'ASYNC' ? `
<svg class="bi" width="24" height="24" fill="currentColor">
  <use xlink:href="assets/bootstrap-icons-1.5.0/bootstrap-icons.svg#exclamation-triangle-fill"/>
</svg>` : '&nbsp';
                let expanded = (query.queryId in this._queryId2Expanded) && this._queryId2Expanded[query.queryId];
                let row_class = expanded ? "row_expanded" : "row_compact";
                let query_compact_class  = expanded ? "hidden"  : "visible";
                let query_expanded_class = expanded ? "visible" : "hidden";
                html += `
<tr class="${failed_query_class} ${row_class}" id="${query.queryId}" title="${queryTitle}">
  <td class="text-success">${attention}</td>
  <td style="padding-right:10px;"><pre>` + query.submitted + `</pre></td>
  <td style="padding-right:10px;"><pre>${query.status}</pre></td>
  <th style="text-align:right; padding-top:0;">${elapsed}</th>
  <td><pre>` + query.qType + `</pre></td>
  <th scope="row" style="text-align:right;"><pre>${query.queryId}</pre></th>
  <td><pre class="query compact ${query_compact_class}"  style="${queryStyle}">` + query.query + `</pre>
      <pre class="query expanded ${query_expanded_class}" style="${queryStyle}">` + sqlFormatter.format(query.query, sqlFormatterConfig) + `</pre>
  </td>
</tr>`;
            }
            this._tablePastQueries().children('tbody').html(html).children("tr").click(toggleQueryDisplay);
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
         * 
         * @param {Number} qid  a unique identifier of a qiery. It's used to pull a record
         * for the previously (of any) recorded number of second estimated before the query
         * would expected to finish.
         * @param {Number} totalSeconds
         * @returns {String} an arrow indicating the trend to slow down or accelerate
         */
        _trend(qid, nextTotalSeconds) {
            if (!_.isUndefined(nextTotalSeconds)) {
                if (this._prevTotalSeconds === undefined) {
                    this._prevTotalSeconds = {};
                }
                let prevTotalSeconds = _.has(this._prevTotalSeconds, qid) ? this._prevTotalSeconds[qid] : nextTotalSeconds;
                this._prevTotalSeconds[qid] = nextTotalSeconds;
                if (prevTotalSeconds < nextTotalSeconds) {
                    return '<span class="trend_up">&nbsp;&uarr;</span>';
                } else if (prevTotalSeconds > nextTotalSeconds) {
                    return '<span class="trend_down">&nbsp;&darr;</span>';
                }
            }
            return '<span>&nbsp;&nbsp;</span>';
        }

        /**
         * @param {integer} chunks
         * @param {integer} totalSeconds
         * @returns {integer} the number of chunks per minute (or 0 if the totalSeconds is 0)
         */
        _performance(chunks, totalSeconds) {
            if (chunks === 0 || totalSeconds === 0) return 0;
            return Math.floor(chunks / (totalSeconds / 60.));
        }
    }
    return StatusUserQueries;
});
