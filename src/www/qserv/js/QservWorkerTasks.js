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
      <div class="form-group col-md-3">
        <label for="worker">Worker:</label>
        <select id="worker" class="form-control form-control-selector">
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="state">State:</label>
        <select id="state" class="form-control form-control-selector">
          <option value="" selected>&lt;any&gt;</option>
          <option value="CREATED">CREATED</option>
          <option value="QUEUED">QUEUED</option>
          <option value="RUNNING">RUNNING</option>
          <option value="FINISHED">FINISHED</option>
          <option value="!FINISHED">!FINISHED</option>
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
          <th class="sticky" style="text-align:center;"><i class="bi bi-clipboard-fill"></i></th>
          <th class="sticky" style="text-align:right;">scanInteractive</th>
          <th class="sticky" style="text-align:right;">jobId</th>
          <th class="sticky" style="text-align:right;">chunkId</th>
          <th class="sticky" style="text-align:right;">attemptId</th>
          <th class="sticky" style="text-align:right;">sequenceId</th>
          <th class="sticky" style="text-align:right;">fragmentId</th>
          <th class="sticky" style="text-align:right;">state</th>
          <th class="sticky" style="text-align:right;">cancelled</th>
          <th class="sticky" style="text-align:right;">sizeSoFar</th>
          <th class="sticky" style="text-align:right;">queueTime</th>
          <th class="sticky" style="text-align:right;">startTime</th>
          <th class="sticky" style="text-align:right;">finishTime</th>
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
                this._set_state('');
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
        _state() { return this._form_control('select', 'state').val(); }
        _set_state(val) { this._form_control('select', 'state').val(val); }
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
                {timeout_sec: 2, version: Common.RestAPIVersion},
                (data) => {
                    this._display(data.status);
                    Fwk.setLastUpdate(this._table().children('caption'));
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
            const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";
            const worker = this._worker();
            const state = this._state();
            let html = '';
            if (!data[worker].success || _.isUndefined(data[worker].info.processor) ||
                                         _.isUndefined(data[worker].info.processor.queries) ||
                                         _.isUndefined(data[worker].info.processor.queries.query_stats)) {
                ;
            } else {
                let query_stats = data[worker].info.processor.queries.query_stats;
                if (!_.isEmpty(query_stats)) {
                    const query_ids = _.keys(query_stats);
                    query_ids.sort();
                    query_ids.reverse();
                    for (let i in query_ids) {
                        const queryId = query_ids[i];
                        let rowspan = 1;
                        let htmlTasks = '';    
                        if (!_.has(query_stats[queryId], "tasks")) continue;
                        let tasks = _.sortBy(query_stats[queryId].tasks, 'state');
                        for (let j in tasks) {
                            let task = tasks[j];
                            // (Optionally) filter tasks by the select state.
                            const allowedState = (state === '') ||
                                                 ((state === '!FINISHED') && (QservWorkerTasks._state2str(task.state) !== 'FINISHED')) ||
                                                 (state === QservWorkerTasks._state2str(task.state));
                            if (!allowedState) continue;
                            htmlTasks += `
<tr class="${QservWorkerTasks._state2css(task.state)}" id="${queryId}">
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
  <td style="text-align:right;"><pre>${task.scanInteractive ? "yes" : "no"}</pre></td>
  <td style="text-align:right;"><pre>${task.jobId}</pre></td>
  <td style="text-align:right;"><pre>${task.chunkId}</pre></td>
  <td style="text-align:right;"><pre>${task.attemptId}</pre></td>
  <td style="text-align:right;"><pre>${task.sequenceId}</pre></td>
  <td style="text-align:right;"><pre>${task.fragmentId}</pre></td>
  <td style="text-align:right;"><pre>${QservWorkerTasks._state2str(task.state)}</pre></td>
  <td style="text-align:right;"><pre>${task.cancelled == "0" ? "no" : "yes"}</pre></td>
  <td style="text-align:right;"><pre>${task.sizeSoFar}</pre></td>
  <td style="text-align:right;"><pre>${task.queueTime}</pre></td>
  <td style="text-align:right;"><pre>${task.startTime}</pre></td>
  <td style="text-align:right;"><pre>${task.finishTime}</pre></td>
</tr>`;
                            rowspan++;
                        }
                        html += `
<tr>
  <th rowspan="${rowspan}" style="text-align:right;"><pre>${queryId}</pre></th>
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
        }
        static _state2css(state) {
            switch (state) {
                case 0: return 'table-light';
                case 1: return 'table-light';
                case 2: return 'table-info';
                case 3: return 'table-success';
                default: return 'table-warning';
            }
        }
        static _state2str(state) {
            switch (state) {
                case 0: return 'CREATED';
                case 1: return 'QUEUED';
                case 2: return 'RUNNING';
                case 3: return 'FINISHED';
                default: return 'UNKNOWN';
            }
        }
    }
    return QservWorkerTasks;
});
