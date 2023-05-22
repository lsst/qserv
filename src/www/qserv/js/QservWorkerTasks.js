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

    CSSLoader.load('qserv/css/QservWorkerTasks.css');

    class QservWorkerTasks extends FwkApplication {

        constructor(name) {
            super(name);
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
            let html = `
<div class="row" id="fwk-qserv-tasks-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="num-tasks"># displayed / selected / total:</label>
        <input type="text" id="num-tasks" class="form-control" value="0 / 0 / 0" disabled>
      </div>
      <div class="form-group col-md-3">
        <label for="worker">Worker:</label>
        <select id="worker" class="form-control form-control-selector">
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="query">Query Id:</label>
        <select id="query" class="form-control form-control-selector">
          <option value="" selected>&lt;any&gt;</option>
        </select>
      </div>
      <div class="form-group col-md-3">
        <label for="state">State:</label>
        <select id="state" class="form-control form-control-selector">
          <option value="">&lt;any&gt;</option>
          <option value="CREATED">CREATED</option>
          <option value="QUEUED">QUEUED</option>
          <option value="EXECUTING_QUERY">EXECUTING_QUERY</option>
          <option value="READING_DATA">READING_DATA</option>
          <option value="EXECUTING_QUERY,READING_DATA" selected>EXECUTING_QUERY | READING_DATA</option>
          <option value="FINISHED">FINISHED</option>
          <option value="CREATED,QUEUED,EXECUTING_QUERY,READING_DATA">!FINISHED</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="max-tasks">Max/query:</label>
        <select id="max-tasks" class="form-control form-control-selector" title="Maximum number of tasks to fetch per query">
          <option value="0">&lt;all&gt;</option>
          <option value="10">10</option>
          <option value="50">50</option>
          <option value="100">100</option>
          <option value="200" selected>200</option>
          <option value="300">300</option>
          <option value="400">400</option>
          <option value="500">500</option>
          <option value="1000">1,000</option>
          <option value="2000">2,000</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="update-interval"><i class="bi bi-arrow-repeat"></i> interval:</label>
        <select id="update-interval" class="form-control form-control-selector">
          <option value="5">5 sec</option>
          <option value="10" selected>10 sec</option>
          <option value="20">20 sec</option>
          <option value="30">30 sec</option>
          <option value="60">1 min</option>
          <option value="120">2 min</option>
          <option value="300">5 min</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="reset-tasks-form">&nbsp;</label>
        <button id="reset-tasks-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-tasks">
      <thead class="thead-light">
        <tr>
          <th class="sticky" style="text-align:right;">QID</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky" style="text-align:center;">inter</th>
          <th class="sticky" style="text-align:right;">job</th>
          <th class="sticky" style="text-align:right;">chunk</th>
          <th class="sticky" style="text-align:right;">sub-ch</th>
          <th class="sticky" style="text-align:right;">attempt</th>
          <th class="sticky" style="text-align:right;">frag</th>
          <th class="sticky" style="text-align:right;">templ</th>
          <th class="sticky" style="text-align:right;">seq</th>
          <th class="sticky" style="text-align:right;">state</th>
          <th class="sticky" style="text-align:right;">cancelled</th>
          <th class="sticky" style="text-align:right;">bytes</th>
          <th class="sticky" style="text-align:right;">created</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky" style="text-align:right;">queued</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky" style="text-align:right;">started</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky" style="text-align:right;">queried</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky" style="text-align:right;">finished</th>
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
            cont.find("button#reset-tasks-form").click(() => {
                this._set_query('');
                this._set_state("EXECUTING_QUERY,READING_DATA");
                this._set_max_tasks(200);
                this._set_update_interval_sec(10);
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
        _query() { return this._form_control('select', 'query').val(); }
        _set_query(val) { this._form_control('select', 'query').val(val); }
        _set_queries(queries) {
            const prev_query = this._query();
            let html = '<option value="" selected>&lt;any&gt;</option>';
            for (let i in queries) {
                const query = queries[i];
                const selected = (_.isEmpty(prev_query) && (i === 0)) ||
                                (!_.isEmpty(prev_query) && (prev_query === query));
                html += `
<option value="${query}" ${selected ? "selected" : ""}>${query}</option>`;
            }
            this._form_control('select', 'query').html(html);
        }
        _state() { return this._form_control('select', 'state').val(); }
        _set_state(val) { this._form_control('select', 'state').val(val); }
        _max_tasks() { return this._form_control('select', 'max-tasks').val(); }
        _set_max_tasks(val) { this._form_control('select', 'max-tasks').val(val); }
        _set_num_tasks(total, selected, displayed) { this._form_control('input', 'num-tasks').val(displayed + ' / ' + selected + ' / ' + total); }
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

        /**
         * Table for displaying tasks that are being run at workers.
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-tasks');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web service then render it to the application's page.
         */
        _load() {
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
                    this._load_tasks();
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _load_tasks() {
            Fwk.web_service_GET(
                "/replication/qserv/worker/status/" + this._worker(),
                {   timeout_sec: 2, version: Common.RestAPIVersion,
                    include_tasks: 1, task_states: this._state(), query_ids: this._query(),
                    max_tasks: this._max_tasks()
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

        /**
         * Display tasks
         */
        _display(data) {
            if (_.isEmpty(data)) return;
            let numTasksTotal = 0;
            let numTasksSelected = 0;
            let numTasksDisplayed = 0;
            let html = '';
            const worker = this._worker();
            if (!data[worker].success || _.isUndefined(data[worker].info.processor) ||
                                         _.isUndefined(data[worker].info.processor.queries) ||
                                         _.isUndefined(data[worker].info.processor.queries.query_stats)) {
                ;
            } else {
                let query_stats = data[worker].info.processor.queries.query_stats;
                if (!_.isEmpty(query_stats)) {
                    const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";
                    const query_ids = _.keys(query_stats);
                    query_ids.sort();
                    query_ids.reverse();
                    // Update a collection of queries in the selector.
                    this._set_queries(query_ids);
                    // Display tasks
                    for (let i in query_ids) {
                        const queryId = query_ids[i];
                        let rowspan = 1;
                        let htmlTasks = '';    
                        if (!_.has(query_stats[queryId], "tasks") || _.isEmpty(query_stats[queryId].tasks.entries)) continue;
                        numTasksTotal += query_stats[queryId].tasks.total;
                        numTasksSelected += query_stats[queryId].tasks.selected;
                        let scanInteractive = false;
                        let tasks = _.sortBy(query_stats[queryId].tasks.entries, 'state');
                        const snapshotTime_msec = query_stats[queryId].tasks.snapshotTime_msec;
                        let prevJobId = -1;
                        for (let j in tasks) {
                            let task = tasks[j];
                            // In theory all tasks of the same query should have it the same.
                            scanInteractive = task.scanInteractive;
                            // Display jobId and chunkId for the first subchunk of a job only. These parameters
                            // should have the same value for all subchunk tasks.
                            let jobId = '';
                            let chunkId = '';
                            if (task.jobId != prevJobId) {
                                prevJobId = task.jobId;
                                jobId = task.jobId;
                                chunkId = task.chunkId;
                            }
                            htmlTasks += `
<tr class="${QservWorkerTasks._state2css(task.state)}">
  <td style="text-align:right;"><pre>${jobId}</pre></td>
  <td style="text-align:right;"><pre>${chunkId}</pre></td>
  <td style="text-align:right;"><pre>${task.subChunkId}</pre></td>
  <td style="text-align:right;"><pre>${task.attemptId}</pre></td>
  <td style="text-align:right;"><pre>${task.fragmentId}</pre></td>
  <td style="text-align:right;"><pre>${task.templateId}</pre></td>
  <td style="text-align:right;"><pre>${task.sequenceId}</pre></td>
  <td style="text-align:right;"><pre>${QservWorkerTasks._state2str(task.state)}</pre></td>
  <td style="text-align:right;"><pre>${task.cancelled == "0" ? "no" : "yes"}</pre></td>
  <td style="text-align:right;"><pre>${task.sizeSoFar}</pre></td>
  <td style="text-align:right;"><pre>${task.createTime_msec ? (new Date(task.createTime_msec)).toISOString() : ""}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerTasks._timestamps_diff2str(task.createTime_msec, task.queueTime_msec, snapshotTime_msec)}</pre></th>
  <td style="text-align:right;"><pre>${task.queueTime_msec ? QservWorkerTasks._timestamp2hhmmss(task.queueTime_msec) : ""}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerTasks._timestamps_diff2str(task.queueTime_msec, task.startTime_msec, snapshotTime_msec)}</pre></th>
  <td style="text-align:right;"><pre>${task.startTime_msec ? QservWorkerTasks._timestamp2hhmmss(task.startTime_msec) : ""}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerTasks._timestamps_diff2str(task.startTime_msec, task.queryTime_msec, snapshotTime_msec)}</pre></th>
  <td style="text-align:right;"><pre>${task.queryTime_msec ? QservWorkerTasks._timestamp2hhmmss(task.queryTime_msec) : ""}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerTasks._timestamps_diff2str(task.queryTime_msec, task.finishTime_msec, snapshotTime_msec)}</pre></th>
  <td style="text-align:right;"><pre>${task.finishTime_msec ? QservWorkerTasks._timestamp2hhmmss(task.finishTime_msec) : ""}</pre></td>
</tr>`;
                            rowspan++;
                            numTasksDisplayed++;
                        }
                        html += `
<tr id="${queryId}">
  <th rowspan="${rowspan}" style="text-align:right; vertical-align: top;"><pre>${queryId}</pre></th>
  <td rowspan="${rowspan}" style="text-align:center; padding-top:0; padding-bottom:0; vertical-align: top;">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
  <td rowspan="${rowspan}" style="text-align:center; vertical-align: top;"><pre>${scanInteractive ? "yes" : "no"}</pre></td>
</tr>` + htmlTasks;
                    }
                }
            }
            let tbody = this._table().children('tbody').html(html);
            let displayQuery  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                Fwk.find("Status", "Query Inspector").set_query_id(queryId);
                Fwk.show("Status", "Query Inspector");
            };
            tbody.find("button.inspect-query").click(displayQuery);
            this._set_num_tasks(numTasksTotal, numTasksSelected, numTasksDisplayed);
        }
        static _state2css(state) {
            switch (state) {
                case 0: return 'table-warning';
                case 1: return 'table-light';
                case 2: return 'table-primary';
                case 3: return 'table-info';
                case 4: return 'table-secondary';
                default: return 'table-warning';
            }
        }
        static _state2str(state) {
            switch (state) {
                case 0: return 'CREATED';
                case 1: return 'QUEUED';
                case 2: return 'EXECUTING_QUERY';
                case 3: return 'READING_DATA';
                case 4: return 'FINISHED';
                default: return 'UNKNOWN';
            }
        }
        static _timestamps_diff2str(begin, end, snapshot) {
            if (!begin) return '';
            if (!end) return ((snapshot - begin) / 1000.0).toFixed(1);
            return ((end - begin) / 1000.0).toFixed(1);
        }
        static _timestamp2hhmmss(ts) {
            return (new Date(ts)).toISOString().substring(11, 19);
        }
    }
    return QservWorkerTasks;
});
