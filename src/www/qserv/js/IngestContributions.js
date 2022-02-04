define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/IngestContributions.css');

    class IngestContributions extends FwkApplication {

        constructor(name) {
            super(name);
            this._data = undefined;
        }

        /// @see FwkApplication.fwk_app_on_show
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /// @see FwkApplication.fwk_app_on_hide
        fwk_app_on_hide() {}

        /// @see FwkApplication.fwk_app_on_update
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

        loadTransaction(database, transactions, id) {
            transactions.sort();
            this._init();
            this._set_database(database);
            this._set_transactions(transactions);
            this._set_trans_id(id);
            this._load();
        }

        /**
         * The first time initialization of the page's layout
         */
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            this._prevTimestamp = 0;

            /* <span style="color:maroon">&sum;</span>&nbsp; */

            let html = `
<div class="row">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="contrib-database">Database:</label>
        <input type="text" id="contrib-database"  class="form-control" disabled value="">
      </div>
      <div class="form-group col-md-1">
        <label for="trans-id">Transaction Id:</label>
        <select id="trans-id" class="form-control"></select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-num-select"># Contrib:</label>
        <input type="text" id="contrib-num-select" class="form-control" value="0 / 0" disabled>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-worker">Worker:</label>
          <select id="contrib-worker" class="form-control form-control-view">
            <option value="" selected></option>
          </select>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-table">Table:</label>
        <select id="contrib-table" class="form-control form-control-view">
          <option value="" selected></option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-chunk">Chunk:</label>
        <input type="number" id="contrib-chunk"  class="form-control form-control-view" value="">
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-overlap">Overlap:</label>
        <select id="contrib-overlap" class="form-control form-control-view">
          <option value="" selected></option>
          <option value="0">No</option>
          <option value="1">Yes</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-async">Type:</label>
        <select id="contrib-async" class="form-control form-control-view">
          <option value="" selected></option>
          <option value="0">SYNC</option>
          <option value="1">ASYNC</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="contrib-status">Status:</label>
        <select id="contrib-status" class="form-control form-control-view">
          <option value="" selected></option>
          <option value="IN_PROGRESS">IN_PROGRESS</option>
          <option value="CREATE_FAILED">CREATE_FAILED</option>
          <option value="START_FAILED">START_FAILED</option>
          <option value="READ_FAILED">READ_FAILED</option>
          <option value="LOAD_FAILED">LOAD_FAILED</option>
          <option value="CANCELLED">CANCELLED</option>
          <option value="FINISHED">FINISHED</option>
          <option value="!FINISHED">! FINISHED</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-stage">Stage (IN_PROGRESS):</label>
        <select id="contrib-stage" class="form-control form-control-view">
          <option value="" selected></option>
          <option value="1:QUEUED">1:QUEUED</option>
          <option value="2:READING_DATA">2:READING_DATA</option>
          <option value="3:LOADING_MYSQL">3:LOADING_MYSQL</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-sort-column">Sort by:</label>
        <select id="contrib-sort-column" class="form-control form-control-view">
          <option value="id" selected>Id</option>
          <option value="worker">Worker</option>
          <option value="table">Table</option>
          <option value="chunk">Chunk</option>
          <option value="status">Status</option>
          <option value="stage">Stage</option>
          <option value="create_time">Created</option>
          <option value="create2start">Created &rarr; Started</option>
          <option value="start_time">Started</option>
          <option value="start2read">Started &rarr; Read</option>
          <option value="read_time">Read</option>
          <option value="read2load">Read &rarr; Loaded</option>
          <option value="load_time">Loaded</option>
          <option value="num_bytes">Bytes</option>
          <option value="num_rows">Rows</option>
          <option value="io_read">Read I/O</option>
          <option value="io_load">Load I/O</option>
          <option value="error">Error</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-sort-order">Sort order:</label>
        <select id="contrib-sort-order" class="form-control form-control-view">
          <option value="ASC" selected>ASC</option>
          <option value="DESC">DESC</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-update-interval"><i class="bi bi-arrow-repeat"></i> interval:</label>
        <select id="contrib-update-interval" class="form-control">
          <option value="10">10 sec</option>
          <option value="20">20 sec</option>
          <option value="30">30 sec</option>
          <option value="60" selected>1 min</option>
          <option value="120">2 min</option>
          <option value="300">5 min</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-reset">&nbsp;</label>
        <button id="contrib-reset" type="button" class="btn btn-primary form-control">Reset Form</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-ingest-contributions">
      <thead class="thead-light">
        <tr>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th>Timing</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th class="right-aligned">Size</th>
          <th></th>
          <th class="right-aligned">I/O</th>
          <th>MB/s</th>
          <th></th>
          <th></th>
        </tr>
        <tr>
          <th class="sticky right-aligned">Id</th>
          <th class="sticky right-aligned">Worker</th>
          <th class="sticky right-aligned">Table</th>
          <th class="sticky right-aligned">Chunk</th>
          <th class="sticky right-aligned">Overlap</th>
          <th class="sticky right-aligned">Type</th>
          <th class="sticky right-aligned">Status</th>
          <th class="sticky">Stage</th>
          <th class="sticky"><elem style="color:red;">&darr;</elem></th>
          <th class="sticky right-aligned">Created</elem></th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky right-aligned">Started</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky right-aligned">Read</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="sticky right-aligned">Loaded</th>
          <th class="sticky right-aligned">Bytes</th>
          <th class="sticky right-aligned">Rows</th>
          <th class="sticky right-aligned">Read</th>
          <th class="sticky right-aligned">Load</th>
          <th class="sticky">Error</th>
          <th class="sticky">Url</th>
        </tr>
      </thead>
      <caption>No transaction</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control#trans-id").change(() => {
                this._load();
            });
            cont.find(".form-control-view").change(() => {
                if (!_.isUndefined(this._data)) this._display(this._data);
            });
            cont.find(".form-control#contrib-update-interval").change(() => {
                this._load();
            });
            cont.find("button#contrib-reset").click(() => {
                this._reset_controls();
                if (!_.isUndefined(this._data)) this._display(this._data);
            });
        }
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-ingest-contributions');
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
        _get_trans_id() { return this._form_control('select', 'trans-id').val(); }
        _set_trans_id(val) { this._form_control('select', 'trans-id').val(val); }
        _set_transactions(transactions) {
            let html = '';
            for (let i in transactions) {
                const id = transactions[i];
                const selected = i ? '' : 'selected'; 
                html += `<option value="${id}" ${selected}>${id}</option>`;
            }
            this._form_control('select', 'trans-id').html(html);
        }
        _reset_controls() {
            this._form_control('select', 'contrib-worker').val('');
            this._form_control('select', 'contrib-table').val('');
            this._form_control('input',  'contrib-chunk').val('');
            this._form_control('select', 'contrib-overlap').val('');
            this._form_control('select', 'contrib-async').val('');
            this._form_control('select', 'contrib-status').val('');
            this._form_control('select', 'contrib-stage').val('');
            this._form_control('select', 'contrib-sort-column').val('id');
            this._form_control('select', 'contrib-sort-order').val('ASC');
            this._form_control('select', 'contrib-update-interval').val('60');
        }
        _disable_controls(disable) {
            this._form_control('select', 'trans-id').prop('disabled', disable);
            this._form_control('select', 'contrib-worker').prop('disabled', disable);
            this._form_control('select', 'contrib-table').prop('disabled', disable);
            this._form_control('input',  'contrib-chunk').prop('disabled', disable);
            this._form_control('select', 'contrib-overlap').prop('disabled', disable);
            this._form_control('select', 'contrib-async').prop('disabled', disable);
            this._form_control('select', 'contrib-status').prop('disabled', disable);
            this._form_control('select', 'contrib-stage').prop('disabled', disable);
            this._form_control('select', 'contrib-sort-column').prop('disabled', disable);
            this._form_control('select', 'contrib-sort-order').prop('disabled', disable);
            this._form_control('select', 'contrib-update-interval').prop('disabled', disable);
            this._form_control('button', 'contrib-reset').prop('disabled', disable);
        }
        _set_num_select(val, total) { this._form_control('input', 'contrib-num-select').val(val + ' / ' + total); }
        _get_database() { return this._form_control('input', 'contrib-database').val(); }
        _set_database(val) { this._form_control('input', 'contrib-database').val(val); }
        _get_worker() { return this._form_control('select', 'contrib-worker').val(); }
        _set_workers(workers, val) {
            let html = `<option value=""></option>`;
            for (let worker in workers) {
                html += `<option value="${worker}">${worker}</option>`;
            }
            this._form_control('select', 'contrib-worker').html(html).val(val);
        }
        _get_table() { return this._form_control('select', 'contrib-table').val(); }
        _set_tables(tables, val) {
            let html = `<option value=""></option>`;
            for (let table in tables) {
                html += `<option value="${table}">${table}</option>`;
            }
            this._form_control('select', 'contrib-table').html(html).val(val);
        }
        _get_chunk() { return this._form_control('input', 'contrib-chunk').val(); }
        _get_overlap() { return this._form_control('select', 'contrib-overlap').val(); }
        _get_async() { return this._form_control('select', 'contrib-async').val(); }
        _get_status() { return this._form_control('select', 'contrib-status').val(); }
        _get_stage() { return this._form_control('select', 'contrib-stage').val(); }
        _get_sort_by_column() { return this._form_control('select', 'contrib-sort-column').val(); }
        _get_sort_order() { return this._form_control('select', 'contrib-sort-order').val(); }
        _update_interval_sec() { return this._form_control('select', 'contrib-update-interval').val(); }

        /**
         * Load data from a web servie then render it to the application's page.
         */
        _load() {
            // Updates make no sense if no transaction identifier provided
            if (!this._get_trans_id()) {
                this._table().children('tbody').html('');
                return;
            }

            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._status().html('<span style="color:maroon">Loading...</span>');
            this._status().addClass('updating');
            this._disable_controls(true);

            Fwk.web_service_GET(
                "/ingest/trans/" + this._get_trans_id(),
                {contrib: 1, contrib_long: 1},
                (data) => {
                    if (!data.success) {
                        this._status().html('<span style="color:maroon">No such transaction</span>');
                        this._table().children('tbody').html('');
                    } else {
                        const MiB = 1024 * 1024;
                        // There should be just one database in the collection.
                        for (let database in data.databases) {
                            this._data = data.databases[database].transactions[0];
                            // The sort order needs to be reset to allow pre-sorting the new data the first
                            // time it will get displayed.
                            this._prev_sort_by_column = undefined;
                            this._prev_sort_order     = undefined;
                            const workers = {};
                            const tables = {};
                            for (let i in this._data.contrib.files) {
                                // INMPORTANT: using 'var' instead of 'let' to allow modifying the content
                                // of the contributions in the original collection. Otherwise mods would
                                // ba made to a local copy of the contribution descriptor which has the life
                                // expectancy not exceeding the body the body of the loop.
                                var file = this._data.contrib.files[i];
                                // Compute the 'stage' attribute of the IN_PROGRESS contribution requests
                                // based on the timestamps.
                                if (file.status === 'IN_PROGRESS') {
                                    if      (!file.start_time) file.stage = '1:QUEUED';
                                    else if (!file.read_time)  file.stage = '2:READING_DATA';
                                    else if (!file.load_time)  file.stage = '3:LOADING_MYSQL';
                                } else {
                                    file.stage = '';
                                }
                                // Compute intervals (put the large numbers for the missing timestamps)
                                file.create2start = file.create_time && file.start_time ? file.start_time - file.create_time : file.create_time;
                                file.start2read   = file.start_time  && file.read_time  ? file.read_time  - file.start_time  : file.create_time;
                                file.read2load    = file.read_time   && file.load_time  ? file.load_time  - file.read_time   : file.create_time;
                                // Compute the I/O performance counters
                                file.io_read = 0;
                                file.io_load = 0;
                                if (file.status === 'FINISHED') {
                                    let readSec = (file.read_time - file.start_time) / 1000.;
                                    let loadSec = (file.load_time - file.read_time)  / 1000.;
                                    file.io_read = readSec > 0 ? (file.num_bytes / MiB) / readSec : 0;
                                    file.io_load = loadSec > 0 ? (file.num_bytes / MiB) / loadSec : 0;
                                }
                                workers[file.worker] = 1;
                                tables[file.table] = 1;
                            }
                            this._set_workers(workers, this._get_worker());
                            this._set_tables(tables, this._get_table());
                            this._display(this._data);
                            break;
                        }
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._disable_controls(false);
                    this._loading = false;
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._disable_controls(false);
                    this._loading = false;
                }
            );
        }

        /**
         * Render the data received from a server
         * @param {Object} info transaction descriptor
         */
        _display(info) {
            // Sort if the first time displaying the data, or if the sort order has changed
            // since the previous run of the display.
            const sort_by_column = this._get_sort_by_column();
            const sort_order     = this._get_sort_order();
            if (_.isUndefined(this._prev_sort_by_column) || (this._prev_sort_by_column !== sort_by_column) ||
                _.isUndefined(this._prev_sort_order)     || (this._prev_sort_order     !== sort_order)) {
                info.contrib.files = _.sortBy(info.contrib.files, sort_by_column);
                if (sort_order === 'DESC') info.contrib.files.reverse();
                this._prev_sort_by_column = sort_by_column;
                this._prev_sort_order     = sort_order;
            }
            const database = this._get_database();
            const worker = this._get_worker();
            const workerIsSet = worker !== '';

            const table = this._get_table();
            const tableIsSet = table !== '';

            const chunkIsSet = this._get_chunk() !== '';
            const chunk = chunkIsSet ? parseInt(this._get_chunk()) : '';

            const overlapIsSet = this._get_overlap() !== '';
            const overlap = overlapIsSet ? parseInt(this._get_overlap()) : '';

            const asyncIsSet = this._get_async() !== '';
            const async = asyncIsSet ? parseInt(this._get_async()) : '';

            const status = this._get_status();
            const statusNotFinishedIsSet = status == '!FINISHED';
            const statusIsSet = status !== '';

            const stage = this._get_stage();
            const stageIsSet = stage !== '';

            let numSelect = 0;
            let html = '';
            for (let idx in info.contrib.files) {
                var file = info.contrib.files[idx];
                // Apply optional content filters
                if (workerIsSet && file.worker !== worker) continue;
                if (tableIsSet && file.table !== table) continue;
                if (chunkIsSet && file.chunk !== chunk) continue;
                if (overlapIsSet && file.overlap !== overlap) continue;
                if (asyncIsSet && file.async !== async) continue;
                if (statusIsSet) {
                    if (statusNotFinishedIsSet) {
                        if (file.status === 'FINISHED') continue;
                    } else if (file.status !== status) continue;
                }
                if (stageIsSet && (file.status === 'IN_PROGRESS') && (file.stage !== stage)) continue;
                numSelect++;
                const overlapStr = file.overlap ? 1 : 0;
                const asyncStr = file.async ? 'ASYNC' : 'SYNC';
                let statusCssClass = '';
                switch (file.status) {
                    case 'FINISHED':    statusCssClass = ''; break;
                    case 'IN_PROGRESS': statusCssClass = 'alert alert-success'; break;
                    default:            statusCssClass = 'alert alert-danger';  break;
                }
                const createDateTimeStr = (new Date(file.create_time)).toLocalTimeString('iso').split(' ');
                const createDateStr = createDateTimeStr[0];
                const createTimeStr = createDateTimeStr[1];
                const startTimeStr  = file.start_time === 0 ? '' : (new Date(file.start_time)).toLocalTimeString('iso').split(' ')[1];
                const readTimeStr   = file.read_time === 0 ? '' : (new Date(file.read_time)).toLocalTimeString('iso').split(' ')[1];
                const loadTimeStr   = file.load_time === 0 ? '' : (new Date(file.load_time)).toLocalTimeString('iso').split(' ')[1];
                const startDeltaStr = file.start_time && file.create_time ? ((file.start_time - file.create_time) / 1000).toFixed(1) : '';
                const readDeltaStr  = file.read_time  && file.start_time  ? ((file.read_time  - file.start_time)  / 1000).toFixed(1) : '';
                const loadDeltaStr  = file.load_time  && file.read_time   ? ((file.load_time  - file.read_time)   / 1000).toFixed(1) : '';
                let readPerfStr = file.io_read ? file.io_read.toFixed(1) : '';
                let loadPerfStr = file.io_load ? file.io_load.toFixed(1) : '';
        html += `
<tr class="${statusCssClass}">
  <th class="right-aligned"><pre>${file.id}</pre></th>
  <td class="right-aligned"><pre>${file.worker}</pre></td>
  <td class="right-aligned"><pre class="database_table" database="${database}" table="${file.table}">${file.table}</pre></td>
  <td class="right-aligned"><pre>${file.chunk}</pre></td>
  <td class="right-aligned"><pre>${overlapStr}</pre></td>
  <td class="right-aligned"><pre>${asyncStr}</pre></th>
  <td class="right-aligned"><pre>${file.status}</pre></td>
  <td><pre>${file.stage}</pre></td>
  <th><pre>${createDateStr}</pre></th>
  <td class="right-aligned"><pre>${createTimeStr}</pre></td>
  <th class="right-aligned"><pre>${startDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${startTimeStr}</pre></td>
  <th class="right-aligned"><pre>${readDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${readTimeStr}</pre></td>
  <th class="right-aligned"><pre>${loadDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${loadTimeStr}</pre></td>
  <td class="right-aligned"><pre>${file.num_bytes}</pre></td>
  <td class="right-aligned"><pre>${file.num_rows}</pre></td>
  <th class="right-aligned"><pre>${readPerfStr}</pre></th>
  <th class="right-aligned"><pre>${loadPerfStr}</pre></th>
  <td style="color:maroon;">${file.error}</td>
  <td><pre>${file.url}</pre></td>
</tr>`;
            }
            this._table().children('tbody').html(html).find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
            this._set_num_select(numSelect, info.contrib.files.length);
        }
    }
    return IngestContributions;
});
