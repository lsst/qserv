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

    CSSLoader.load('qserv/css/IngestContributions.css');

    class IngestContributions extends FwkApplication {

        constructor(name) {
            super(name);
            this._files = [];
            this._max_num_workers = 0;
            this._max_num_tables = 0;
            this._max_num_trans = 0;
        }

        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        fwk_app_on_hide() {}

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

        /// Load and display contributions in the specified context.
        search(worker = undefined, database = undefined, table = undefined, transaction = undefined, status = undefined) {
            this._init();
            this._set_status(status === undefined ? 'IN_PROGRESS' : status);
            this._load(worker, database, table, transaction);
        }

        /**
         * The first time initialization of the page's layout
         */
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            this._prevTimestamp = 0;
            let html = `
<div class="row" id="fwk-ingest-contributions-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="contrib-worker">Worker:</label>
          <select id="contrib-worker" class="form-control loader">
            <option value="" selected></option>
          </select>
      </div>
      <div class="form-group col-md-3">
        <label for="contrib-database">Database:</label>
        <select id="contrib-database" class="form-control loader"></select>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-table">Table:</label>
        <select id="contrib-table" class="form-control loader">
          <option value="" selected></option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-transaction">Transaction Id:</label>
        <select id="contrib-transaction" class="form-control loader"></select>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-status">Status:</label>
        <select id="contrib-status" class="form-control loader">
          <option value="">&lt;any&gt;</option>
          <option value="IN_PROGRESS" selected>IN_PROGRESS</option>
          <option value="CREATE_FAILED">CREATE_FAILED</option>
          <option value="START_FAILED">START_FAILED</option>
          <option value="READ_FAILED">READ_FAILED</option>
          <option value="LOAD_FAILED">LOAD_FAILED</option>
          <option value="CANCELLED">CANCELLED</option>
          <option value="FINISHED">FINISHED</option>
          <option value="!FINISHED">!&nbsp;FINISHED</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-stage">IN_PROGRESS stage:</label>
        <select id="contrib-stage" class="form-control filter">
          <option value="">&lt;any&gt;</option>
          <option value="QUEUED">QUEUED</option>
          <option value="!QUEUED" selected>!&nbsp;QUEUED</option>
          <option value="READING_DATA">READING_DATA</option>
          <option value="LOADING_MYSQL">LOADING_MYSQL</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="contrib-chunk">Chunk:</label>
        <input type="number" id="contrib-chunk"  class="form-control loader" value="">
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-overlap">Overlap:</label>
        <select id="contrib-overlap" class="form-control filter">
          <option value="" selected>&lt;any&gt;</option>
          <option value="0">No</option>
          <option value="1">Yes</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-async">Type:</label>
        <select id="contrib-async" class="form-control filter">
          <option value="" selected>&lt;any&gt;</option>
          <option value="0">SYNC</option>
          <option value="1">ASYNC</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-min-retries">Min.Retries:</label>
        <input type="number" id="contrib-min-retries"  class="form-control loader" value="0">
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-min-warnings">Min.Warnings:</label>
        <input type="number" id="contrib-min-warnings"  class="form-control loader" value="0">
      </div>
      <div class="form-group col-md-1">
        <label for="max-entries">Max/trans:</label>
        <select id="max-entries" class="form-control loader" title="Maximum number of contributions to fetch per transaction">
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
          <option value="3000">3,000</option>
          <option value="5000">5,000</option>
          <option value="10000">10,000</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 60)}
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-reset">&nbsp;</label>
        <button id="contrib-reset" type="button" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="contrib-sort-column">Sort by:</label>
        <select id="contrib-sort-column" class="form-control sorter">
          <option value="id" selected>Id</option>
          <option value="worker">Worker</option>
          <option value="table">Table</option>
          <option value="trans_id">Trans.Id</option>
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
          <option value="num_rows">Rows parsed</option>
          <option value="num_rows_loaded">Rows</option>
          <option value="io_read">Read I/O</option>
          <option value="io_load">Load I/O</option>
          <option value="error">Error</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="contrib-sort-order">Sort order:</label>
        <select id="contrib-sort-order" class="form-control sorter">
          <option value="ASC" selected>ASC</option>
          <option value="DESC">DESC</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="contrib-num-workers"># workers:</label>
        <input type="text" id="contrib-num-workers" class="form-control" value="0 / 0" disabled>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-num-tables"># tables:</label>
        <input type="text" id="contrib-num-tables" class="form-control" value="0 / 0" disabled>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-num-trans"># trans:</label>
        <input type="text" id="contrib-num-trans" class="form-control" value="0 / 0" disabled>
      </div>
      <div class="form-group col-md-2">
        <label for="contrib-num-select"># filtered contribs:</label>
        <input type="text" id="contrib-num-select" class="form-control" value="0 / 0" disabled>
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
          <th class="right-aligned">Rows</th>
          <th class="right-aligned">I/O</th>
          <th>MB/s</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
        <tr>
          <th class="sticky right-aligned">Id</th>
          <th class="sticky right-aligned">Worker</th>
          <th class="sticky right-aligned">Trans</th>
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
          <th class="sticky right-aligned">Parsed</th>
          <th class="sticky right-aligned">Loaded</th>
          <th class="sticky right-aligned">Read</th>
          <th class="sticky right-aligned">Load</th>
          <th class="sticky right-aligned">Retries</th>
          <th class="sticky right-aligned">Warnings</th>
          <th class="sticky">Error</th>
          <th class="sticky">Url</th>
        </tr>
      </thead>
      <caption>Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find("#update-interval").on("change", () => {
                this._load();
            });
            cont.find(".loader").on("change", (ev) => {
                if (ev.target.id === "contrib-status") {
                    if (this._get_status() === "IN_PROGRESS") {
                        this._disable_stage(false);
                    } else {
                      this._disable_stage(true);
                      this._set_stage("");
                    }
                }
                this._load();
            });
            cont.find(".filter").on("change", () => {
                this._display();
            });
            cont.find(".sorter").on("change", () => {
              this._display();
            });
            cont.find("button#contrib-reset").click(() => {
                this._reset();
                this._load();
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
        _get_database() { return this._form_control('select', 'contrib-database').val(); }
        _set_database(val) { this._form_control('select', 'contrib-database').val(val); }
        _set_databases(databases, database=undefined, table=undefined) {
            databases = _.sortBy(databases, function (info) { return info.database; });
            // Keep the current selection after updating the selector in case if the
            // database belongs to this collection.
            const current_database = _.isUndefined(database) ? this._get_database() : database;
            let in_collection = false;
            this._form_control('select', 'contrib-database').html(
                _.reduce(databases, (html, info) => {
                    if (info.database === current_database) in_collection = true;
                    const selected = !html ? 'selected' : '';
                    return html + `<option value="${info.database}" ${selected}>${info.database}</option>`;
                }, '')
            );
            if (in_collection) this._set_database(current_database);
            // Update the tables selector.
            const current_database_info = _.find(databases, (info) => { return info.database === this._get_database(); });
            const tables = current_database_info ? current_database_info.tables : [];
            this._max_num_tables = _.size(tables);
            this._set_tables(tables, table);
        }

        _get_transaction() { return this._form_control('select', 'contrib-transaction').val(); }
        _set_transaction(val) { this._form_control('select', 'contrib-transaction').val(val); }
        _set_transactions(transactions, transaction=undefined) {
            // Keep the current selection after updating the selector in case if the
            // transaction belongs to this collection.
            const current_id = parseInt(_.isUndefined(transaction) ? this._get_transaction() : transaction);
            let in_collection = false;
            this._form_control('select', 'contrib-transaction').html(
              `<option value="0" selected>&lt;any&gt;</option>` +
              _.reduce(transactions, (html, id) => {
                    if (id === current_id) in_collection = true;
                    return html + `<option value="${id}">${id}</option>`;
                }, '')
            );
            if (in_collection) this._set_transaction(current_id);
        }

        _set_num_workers(val) { this._form_control('input', 'contrib-num-workers').val(val + ' / ' + this._max_num_workers); }
        _set_num_tables(val) { this._form_control('input', 'contrib-num-tables').val(val + ' / ' + this._max_num_tables); }
        _set_num_trans(val) { this._form_control('input', 'contrib-num-trans').val(val + ' / ' + this._max_num_trans); }
        _set_num_select(val) { this._form_control('input', 'contrib-num-select').val(val + ' / ' + this._get_max_entries()); }

        _get_min_retries() { return this._form_control('input', 'contrib-min-retries').val(); }
        _set_min_retries(val) { this._form_control('input', 'contrib-min-retries').val(val); }

        _get_min_warnings() { return this._form_control('input', 'contrib-min-warnings').val(); }
        _set_min_warnings(val) { this._form_control('input', 'contrib-min-warnings').val(val); }

        _get_max_entries() { return this._form_control('select', 'max-entries').val(); }
        _set_max_entries(val) { this._form_control('select', 'max-entries').val(val); }

        _get_worker() { return this._form_control('select', 'contrib-worker').val(); }
        _set_worker(val) { this._form_control('select', 'contrib-worker').val(val); }
        _set_workers(workers, worker=undefined) {
            // Keep the current selection after updating the selector in case if the
            // worker belongs to this collection.
            const current_worker = _.isUndefined(worker) ? this._get_worker() : worker;
            let in_collection = false;
            this._form_control('select', 'contrib-worker').html(
                `<option value="" selected>&lt;any&gt;</option>` +
                _.reduce(workers, (html, info) => {
                    if (info.name === current_worker) in_collection = true;
                    return html + `<option value="${info.name}">${info.name}</option>`;
                }, '')
            );
            if (in_collection) this._set_worker(current_worker);
        }

        _get_table() { return this._form_control('select', 'contrib-table').val(); }
        _set_table(val) { this._form_control('select', 'contrib-table').val(val); }
        _set_tables(tables, table=undefined) {
            // Keep the current selection after updating the selector in case if the
            // table belongs to this collection.
            const current_table = _.isUndefined(table) ? this._get_table() : table;
            let in_collection = false;
            this._form_control('select', 'contrib-table').html(
                `<option value="" selected>&lt;any&gt;</option>` +
                _.reduce(tables, (html, info) => {
                    if (info.name === current_table) in_collection = true;
                    return html + `<option value="${info.name}">${info.name}</option>`;
                }, '')
            );
            if (in_collection) this._set_table(current_table);
        }

        _get_chunk() {
            const val = this._form_control('input', 'contrib-chunk').val();
            return _.isEmpty(val) ? '-1' : val;
        }
        _set_chunk(val) { this._form_control('input', 'contrib-chunk').val(val); }

        _get_overlap() { return this._form_control('select', 'contrib-overlap').val(); }
        _set_overlap(val) { this._form_control('select', 'contrib-overlap').val(val); }

        _get_async() { return this._form_control('select', 'contrib-async').val(); }
        _set_async(val) { this._form_control('select', 'contrib-async').val(val); }

        _get_status() { return this._form_control('select', 'contrib-status').val(); }
        _set_status(val) { this._form_control('select', 'contrib-status').val(val); }

        _get_stage() { return this._form_control('select', 'contrib-stage').val(); }
        _set_stage(val) { this._form_control('select', 'contrib-stage').val(val); }
        _disable_stage(yes) { this._form_control('select', 'contrib-stage').prop('disabled', yes); }

        _get_sort_by_column() { return this._form_control('select', 'contrib-sort-column').val(); }
        _get_sort_order() { return this._form_control('select', 'contrib-sort-order').val(); }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }

        _reset() {
          this._set_worker('');
          this._set_table('');
          this._set_transaction('');
          this._set_chunk('');
          this._set_overlap('');
          this._set_async('');
          this._set_status('IN_PROGRESS');
          this._set_stage('!QUEUED');
          this._disable_stage(false);
          this._set_min_retries(0);
          this._set_min_warnings(0);
          this._set_max_entries(200);
      }
      _disable_controls(disable) {
          this.fwk_app_container.find(".form-control.loader").prop('disabled', disable);
      }

      _load(worker, database, table, transaction) {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            this._disable_controls(true);
            this._load_workers(worker, database, table, transaction);
        }
        _load_workers(worker, database, table, transaction) {
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                    } else {
                        this._max_num_workers = _.size(data.config.workers);
                        this._set_workers(data.config.workers, worker);
                        this._load_databases(database, table, transaction);
                    }
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_databases(database, table, transaction) {
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                    } else {
                        this._set_databases(data.config.databases, database, table);
                        this._load_transactions(transaction);
                    }
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_transactions(transaction) {
            const current_database = this._get_database();
            if (!current_database) {
                this._on_failure("No databases found in this status category");
                return;
            }
            Fwk.web_service_GET(
                "/ingest/trans",
                {database: current_database, contrib: 0, contrib_long: 0, version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    // Transactions are shown sorted in the ASC order
                    this._set_transactions(
                        _.map(
                            _.sortBy(data.databases[current_database].transactions, function (info) { return info.id; }),
                            function (info) { return info.id; }
                        ),
                        transaction
                    );
                    this._load_contribs();
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_contribs() {
            Fwk.web_service_GET(
                "/ingest/trans/" + this._get_transaction(),
                {   contrib: 1,
                    database: this._get_database(),
                    table: this._get_table(),
                    worker: this._get_worker(),
                    chunk: this._get_chunk(),
                    contrib_status: this._get_status(),
                    contrib_long: 1,
                    min_retries: this._get_min_retries(),
                    min_warnings: this._get_min_warnings(),
                    max_entries: this._get_max_entries(),
                    version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._status().html('<span style="color:maroon">No such transaction</span>');
                        this._table().children('tbody').html('');
                    } else {
                        this._on_contrib_loaded(data);
                        Fwk.setLastUpdate(this._status());
                    }
                    this._status().removeClass('updating');
                    this._disable_controls(false);
                    this._loading = false;
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _on_failure(msg) {
            this._status().html(`<span style="color:maroon">${msg}</span>`);
            this._table().children('tbody').html('');
            this._status().removeClass('updating');
            this._disable_controls(false);
            this._loading = false;
        }
        _on_contrib_loaded(data) {
            // Preprocess and cache the data in the form suitable for the display.
            //
            // IMPORTANT: using 'var' instead of 'let' to allow modifying the content
            //    of the contributions in the original collection. Otherwise modifications would
            //    be made to a local copy of the contribution descriptor which has the life
            //    expectancy not exceeding the body the body of the enclosing block.
            const MiB = 1024 * 1024;
            this._files = [];

            // The unique scopes for contribution.
            let unique_workers = {};
            let unique_tables = {};
            let unique_trans = {};

            // There should be just one database in the collection.
            var database_data = data.databases[this._get_database()];
            this._max_num_trans = _.size(database_data.transactions);

            for (let i in database_data.transactions) {
                var contrib = database_data.transactions[i].contrib;
                const trans_id = database_data.transactions[i].id;
                // The sort order needs to be reset to allow pre-sorting the new data the first
                // time it will get displayed.
                this._prev_sort_by_column = undefined;
                this._prev_sort_order     = undefined;

                for (let i in contrib.files) {
                    var file = contrib.files[i];
                    file.trans_id = trans_id;
                    // Count contriutions scopes only if they are not already in the collection.
                    unique_workers[file.worker] = true;
                    unique_tables[file.table] = true;
                    unique_trans[trans_id] = true;
                    // Compute the 'stage' attribute of the IN_PROGRESS contribution requests
                    // based on the timestamps.
                    if (file.status === 'IN_PROGRESS') {
                        if      (!file.start_time) file.stage = 'QUEUED';
                        else if (!file.read_time)  file.stage = 'READING_DATA';
                        else if (!file.load_time)  file.stage = 'LOADING_MYSQL';
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
                }
                this._files = this._files.concat(contrib.files);
            }
            this._set_num_workers(_.size(unique_workers));
            this._set_num_tables(_.size(unique_tables));
            this._set_num_trans(_.size(unique_trans));
            this._set_num_select(_.size(this._files));
            this._display();
        }
        _display() {
            // Sort if the first time displaying the data, or if the sort order has changed
            // since the previous run of the display.
            const sort_by_column = this._get_sort_by_column();
            const sort_order     = this._get_sort_order();
            if (_.isUndefined(this._prev_sort_by_column) || (this._prev_sort_by_column !== sort_by_column) ||
                _.isUndefined(this._prev_sort_order)     || (this._prev_sort_order     !== sort_order)) {
                this._files = _.sortBy(this._files, sort_by_column);
                if (sort_order === 'DESC') this._files.reverse();
                this._prev_sort_by_column = sort_by_column;
                this._prev_sort_order     = sort_order;
            }
            const database = this._get_database();

            const overlapIsSet = this._get_overlap() !== '';
            const overlap = overlapIsSet ? parseInt(this._get_overlap()) : '';

            const asyncIsSet = this._get_async() !== '';
            const async = asyncIsSet ? parseInt(this._get_async()) : '';
            
            const stage = this._get_stage();
            const stageIsSet = stage !== '';

            let html = '';
            for (let idx in this._files) {
                var file = this._files[idx];
                // Apply optional content filters
                if (overlapIsSet && file.overlap !== overlap) continue;
                if (asyncIsSet && file.async !== async) continue;
                if (stageIsSet && (file.status === 'IN_PROGRESS')) {
                    if (stage === '!QUEUED') {
                        if ((file.stage !== 'READING_DATA') && (file.stage !== 'LOADING_MYSQL')) continue;
                    } else if (file.stage !== stage) continue;
                }
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
  <th class="right-aligned"><pre class="contrib_id" id="${file.id}">${file.id}</pre></th>
  <td class="right-aligned"><pre>${file.worker}</pre></td>
  <td class="right-aligned"><pre>${file.trans_id}</pre></td>
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
  <td class="right-aligned"><pre>${file.num_rows_loaded}</pre></td>
  <th class="right-aligned"><pre>${readPerfStr}</pre></th>
  <th class="right-aligned"><pre>${loadPerfStr}</pre></th>
  <td class="right-aligned"><pre>${file.num_failed_retries}</pre></td>
  <td class="right-aligned"><pre>${file.num_warnings}</pre></td>
  <td style="color:maroon;">${file.error}</td>
  <td><pre>${file.url}</pre></td>
</tr>`;
            }
            let tbody = this._table().children('tbody');
            tbody.html(html);
            tbody.find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
            tbody.find("pre.contrib_id").click((e) => {
                const id = $(e.currentTarget).attr("id");
                Fwk.find("Ingest", "Contribution Info").set_contrib_id(id);
                Fwk.show("Ingest", "Contribution Info");
            });
        }
    }
    return IngestContributions;
});
