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

    CSSLoader.load('qserv/css/StatusActiveQueries.css');

    class StatusActiveQueries extends FwkApplication {

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

            this._scheduler2color = {
                'Snail':   '#007bff',
                'Slow':    '#17a2b8',
                'Med':     '#28a745',
                'Fast':    '#ffc107',
                'Group':   '#dc3545',
                'Loading': 'default'
            };

            let html = `
<div class="row" id="fwk-status-active-queries-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 5)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-queries-form">&nbsp;</label>
        <button id="reset-queries-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>          
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-status-active-queries">
      <caption class="updating">
        Loading...
      </caption>
      <thead class="thead-light">
        <tr>
          <th class="sticky">Started</th>
          <th class="sticky">Progress</th>
          <th class="sticky">Sched</th>
          <th class="sticky" style="text-align:right;">Elapsed</th>
          <th class="sticky" style="text-align:right;">Left (est.)</th>
          <th class="sticky" style="text-align:right;">Chunks</th>
          <th class="sticky" style="text-align:right;">Ch/min</th>
          <th class="sticky" style="text-align:right;">QID</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-clipboard-fill"></i></th>
          <th class="sticky" class="sticky" style="text-align:center;"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky" class="sticky" style="text-align:center;"><i class="bi bi-bar-chart-steps"></i></th>
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
                this._set_update_interval_sec(5);
                this._load();
          });
        }
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-status-active-queries');
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
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }
        _set_update_interval_sec(val) { this._form_control('select', 'update-interval').val(val); }

        _load() {
            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._status().addClass('updating');

            console.log('_load:1');

            Fwk.web_service_GET(
                "/replication/qserv/master/queries/active",
                {   version: Common.RestAPIVersion,
                    timeout_sec: StatusActiveQueries._server_proc_timeout_sec()
                },
                (data) => {
                    console.log('_load:2');
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
            const queryProgressTitle = "Click to see query progression plot.";
            const queryStyle = "color:#4d4dff;";
            let html = '';
            for (let i in data.queries) {
                let query = data.queries[i];
                this._id2query[query.queryId] = query.query;
                const progress = Math.floor(100. * query.completedChunks  / query.totalChunks);
                const scheduler = _.isUndefined(query.scheduler) ? 'Loading...' : query.scheduler.substring('Sched'.length);
                const scheduler_color = _.has(this._scheduler2color, scheduler) ?
                    this._scheduler2color[scheduler] :
                    this._scheduler2color['Loading'];

                const elapsed = this._elapsed(query.samplingTime_sec - query.queryBegin_sec);
                let leftSeconds;
                if (query.completedChunks > 0 && query.samplingTime_sec - query.queryBegin_sec > 0) {
                    leftSeconds = Math.floor(
                            (query.totalChunks - query.completedChunks) /
                            (query.completedChunks / (query.samplingTime_sec - query.queryBegin_sec))
                    );
                }
                const left = this._elapsed(leftSeconds);
                const trend = this._trend(query.queryId, leftSeconds);
                const performance = this._performance(query.completedChunks, query.samplingTime_sec - query.queryBegin_sec);
                const expanded = (query.queryId in this._queryId2Expanded) && this._queryId2Expanded[query.queryId];
                html += `
<tr id="${query.queryId}">
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
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-dark btn-sm copy-query" style="height:20px; margin:0px;" title="${queryCopyTitle}"></button>
  </td>
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm query-progress" style="height:20px; margin:0px;" title="${queryProgressTitle}"></button>
  </td>
  <td class="query_toggler" title="${queryToggleTitle}"><pre class="query" style="${queryStyle}">` + this._query2text(query.queryId, expanded) + `</pre></td>
</tr>`;
            }
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
            let displayQueryProgress  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                Fwk.find("Czar", "Query Progress").set_query_id(queryId);
                Fwk.show("Czar", "Query Progress");
            };
            let tbodyQueries = this._table().children('tbody').html(html);
            tbodyQueries.find("td.query_toggler").click(toggleQueryDisplay);
            tbodyQueries.find("button.copy-query").click(copyQueryToClipboard);
            tbodyQueries.find("button.inspect-query").click(displayQuery);
            tbodyQueries.find("button.query-progress").click(displayQueryProgress);
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
            if (chunks === 0 || totalSeconds <= 0) return 0;
            return Math.floor(chunks / (totalSeconds / 60.));
        }
        _query2text(queryId, expanded) {
            return Common.query2text(this._id2query[queryId], expanded);
        }
    }
    return StatusActiveQueries;
});
