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

    CSSLoader.load('qserv/css/QservWorkerFiles.css');

    class QservWorkerFiles extends FwkApplication {

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
        set_worker(worker) {
            this._init();
            this._load(worker);
        }
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="row" id="fwk-qserv-files-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="num-files"># displayed / selected / total:</label>
        <input type="text" id="num-files" class="form-control" value="0 / 0 / 0" disabled>
      </div>
      <div class="form-group col-md-2">
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
      <div class="form-group col-md-1">
        <label for="max-files">Max files:</label>
        <select id="max-files" class="form-control form-control-selector" title="Maximum number of files to fetch">
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
        ${Common.html_update_ival('update-interval', 10)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-files-form">&nbsp;</label>
        <button id="reset-files-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-files">
      <thead class="thead-light">
        <tr>
          <th class="sticky" style="text-align:right;">QID</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky" style="text-align:right;">chunk</th>
          <th class="sticky" style="text-align:right;">job</th>
          <th class="sticky" style="text-align:right;">attempt</th>
          <th class="sticky" style="text-align:right;">filename</th>
          <th class="sticky" style="text-align:right;">size</th>
          <th class="sticky" style="text-align:right;">s<sup>-1</sup></th>
          <th class="sticky" style="text-align:right;">created</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky" style="text-align:right;">modified</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky" style="text-align:right;">inspected</th>
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
            cont.find("button#reset-files-form").click(() => {
                this._set_query('');
                this._set_max_files(200);
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
            console.log("prev_query", prev_query, "queries", queries);
            let html = '<option value="">&lt;any&gt;</option>';
            for (let i in queries) {
                const query = queries[i];
                const selected = (!_.isEmpty(prev_query) && (prev_query == query));
                html += `
<option value="${query}" ${selected ? "selected" : ""}>${query}</option>`;
            }
            this._form_control('select', 'query').html(html);
        }
        _max_files() { return this._form_control('select', 'max-files').val(); }
        _set_max_files(val) { this._form_control('select', 'max-files').val(val); }
        _set_num_files(total, selected, displayed) { this._form_control('input', 'num-files').val(displayed + ' / ' + selected + ' / ' + total); }
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
         * Table for displaying files that exist at the worker.
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-files');
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
                    this._load_files();
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _load_files() {
            Fwk.web_service_GET(
                "/replication/qserv/worker/files/" + this._worker(),
                {   timeout_sec: 2, version: Common.RestAPIVersion,
                    query_ids: this._query(),
                    max_files: this._max_files()
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
            // Update a collection of queries in the selector.
            const query_ids = _.uniq(_.map(status.files, function(file) { return file.task.query_id; }));
            query_ids.sort();
            this._set_queries(query_ids);
            // Turn the collection of files into a dictionary
            const files = _.groupBy(status.files, function(file) { return file.task.query_id; });
            console.log(files);
            const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";

            // Display files
            let html = '';
            for (let queryId in files) {
                const queryFiles = _.sortBy(files[queryId], function(file) { return file.task.chunk_id; });
                console.log(queryFiles);
                let rowspan = 1;
                let htmlFiles = '';    
                for (let i in queryFiles) {
                    const file = queryFiles[i];
                    const createTime_msec = file.ctime * 1000;
                    const modifyTime_msec = file.mtime * 1000;
                    const snapshotTime_msec = file.current_time_ms;
                    htmlFiles += `
<tr>
  <td style="text-align:right;"><pre>${file.task.chunk_id}</pre></td>
  <td style="text-align:right;"><pre>${file.task.job_id}</pre></td>
  <td style="text-align:right;"><pre>${file.task.attemptcount}</pre></td>
  <td style="text-align:right;"><pre>${file.filename}</pre></td>
  <td style="text-align:right;"><pre>${file.size}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerFiles._io_performance(file.size, file.ctime, file.mtime)}</pre></th>
  <td style="text-align:right;"><pre>${(new Date(createTime_msec)).toISOString()}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerFiles._timestamps_diff2str(createTime_msec, modifyTime_msec)}</pre></th>
  <td style="text-align:right;"><pre>${QservWorkerFiles._timestamp2hhmmss(modifyTime_msec)}</pre></td>
  <th style="text-align:right;"><pre>${QservWorkerFiles._timestamps_diff2str(modifyTime_msec, snapshotTime_msec)}</pre></th>
  <td style="text-align:right;"><pre>${QservWorkerFiles._timestamp2hhmmss(snapshotTime_msec)}</pre></td>
</tr>`;
                    rowspan++;
                }
                html += `
<tr id="${queryId}">
  <th rowspan="${rowspan}" style="text-align:right; vertical-align: top;"><pre>${queryId}</pre></th>
  <td rowspan="${rowspan}" style="text-align:center; padding-top:0; padding-bottom:0; vertical-align: top;">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
</tr>` + htmlFiles;

            }
            let tbody = this._table().children('tbody').html(html);
            let displayQuery  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                Fwk.find("Status", "Query Inspector").set_query_id(queryId);
                Fwk.show("Status", "Query Inspector");
            };
            tbody.find("button.inspect-query").click(displayQuery);
            this._set_num_files(status.num_total, status.num_selected, status.files.length);
        }
        static _timestamps_diff2str(begin, end, snapshot) {
            return ((end - begin) / 1000.0).toFixed(1);
        }
        static _timestamp2hhmmss(ts) {
            return (new Date(ts)).toISOString().substring(11, 19);
        }
        static _io_performance(size, begin_time_sec, end_time_sec) {
            let bytes_per_sec = size;
            let interval = end_time_sec - begin_time_sec;
            if (interval > 0) bytes_per_sec = size / interval;
            return Common.format_data_rate(bytes_per_sec);
        }
    }
    return QservWorkerFiles;
});
