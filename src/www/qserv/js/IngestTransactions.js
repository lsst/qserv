define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/IngestTransactions.css');

    class IngestTransactions extends FwkApplication {

        constructor(name) {
            super(name);
            // The collections of names/identifiers will be passed to the dependent pages
            // when selecting actions on transactions.
            this._transactions = [];
            this._databases = [];
        }

        /// @see FwkApplication.fwk_app_on_show
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /// @see FwkApplication.fwk_app_on_hide
        fwk_app_on_hide() {
        }

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

        /**
         * The first time initialization of the page's layout
         */
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            this._prevTimestamp = 0;
            let html = `
<div class="form-row" id="fwk-ingest-transactions-controls">
  <div class="form-group col-md-1">
    <label for="trans-database-status">Status:</label>
    <select id="trans-database-status" class="form-control form-control-view">
      <option value=""></option>
      <option value="INGESTING" selected>INGESTING</option>
      <option value="PUBLISHED">PUBLISHED</option>
    </select>
  </div>
  <div class="form-group col-md-3">
    <label for="trans-database">Database:</label>
    <select id="trans-database" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-1">
    <label for="trans-update-interval">Interval <i class="bi bi-arrow-repeat"></i></label>
    <select id="trans-update-interval" class="form-control form-control-view">
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
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-ingest-transactions">
      <thead class="thead-light">
        <tr>
          <th rowspan="2" class="left-aligned">More...</th>
          <th rowspan="2" class="right-aligned">Id</th>
          <th rowspan="2" class="center-aligned">State</th>
          <th colspan="8" class="left-aligned">Timing</th>
          <th colspan="7" class="left-aligned">Contributions</th>
        </tr>
        <tr>
          <th class="right-aligned"><elem style="color:red;">&darr;</elem></th>
          <th class="right-aligned">Created</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="right-aligned">Started</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="right-aligned">Transitioned</th>
          <th class="sticky right-aligned"><elem style="color:red;">&rarr;</elem></th>
          <th class="right-aligned">Ended</th>
          <th class="right-aligned">Workers</th>
          <th class="right-aligned">Reg.</th>
          <th class="right-aligned">Chunks</th>
          <th class="right-aligned">Overlaps</th>
          <th class="right-aligned">Files</th>
          <th class="right-aligned">Rows</th>
          <th class="right-aligned">Data [GB]</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-view").change(() => {
                this._load();
            });
            this._disable_selectors(true);
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _get_database_status() { return this._form_control('select', 'trans-database-status').val(); }
        _get_database() { return this._form_control('select', 'trans-database').val(); }
        _set_database(val) { this._form_control('select', 'trans-database').val(val); }
        _set_databases(databases) {
            // Keep the current selection after updating the selector in case if the
            // database belongs to this collection.
            const current_database = this._get_database();
            let in_collection = false;
            this._form_control('select', 'trans-database').html(
                _.reduce(
                    databases,
                    (html, name) => {
                        if (name === current_database) in_collection = true;
                        const selected = !html ? 'selected' : ''; 
                        return html + `<option value="${name}" ${selected}>${name}</option>`;
                    },
                    ''
                )
            );
            if (in_collection && current_database) this._set_database(current_database);
        }
        _disable_selectors(disable) {
            this.fwk_app_container.find(".form-control-view").prop('disabled', disable);
        }
        _update_interval_sec() { return this._form_control('select', 'trans-update-interval').val(); }
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-ingest-transactions');
            }
            return this._table_obj;
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this._table().children('caption');
            }
            return this._status_obj;
        }
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            this._disable_selectors(true);
            this._load_databases(this._get_database_status());
        }
        _load_databases(status) {
            Fwk.web_service_GET(
                "/replication/config",
                {},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._databases = _.map(
                        _.filter(
                            data.config.databases,
                            function (info) {
                                return (status === "") ||
                                       ((status === "PUBLISHED") && info.is_published) ||
                                       ((status === "INGESTING") && !info.is_published);
                            }
                        ),
                        function (info) { return info.database; }
                    );
                    this._set_databases(this._databases);
                    this._load_transactions();
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_transactions() {
            const current_database = this._get_database();
            if (!current_database) {
                this._on_failure("No databases found in this status category");
                return;
            }
            Fwk.web_service_GET(
                "/ingest/trans",
                {database: current_database, contrib: 1, contrib_long: 0},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._display(data.databases[current_database]);
                    this._disable_selectors(false);
                    Fwk.setLastUpdate(this._status());
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _on_failure(msg) {
            this._status().html(`<span style="color:maroon">${msg}</span>`);
            this._table().children('tbody').html('');
            this._disable_selectors(false);
            this._status().removeClass('updating');
            this._loading = false;
        }
        _display(databaseInfo) {
            let html = '';
            if (databaseInfo.transactions.length === 0) {
                html += `
<tr>
  <th>&nbsp;</th>
  <td>&nbsp;</th>
  <td>&nbsp;</th>
  <td>&nbsp;</th>
</tr>`;
            } else {
                this._transactions = [];
                html += _.reduce(
                    // Transactions are shown sorted in the ASC order
                    _.sortBy(databaseInfo.transactions, function (info) { return info.id; }),
                    (html, info) => {
                        this._transactions.push(info.id);
                        let transactionCssClass = 'bg-white';
                        switch (info.state) {
                            case 'STARTED':  transactionCssClass = 'bg-transparent'; break;
                            case 'FINISHED': transactionCssClass = 'alert alert-success'; break;
                            case 'ABORTED':  transactionCssClass = 'alert alert-danger'; break;
                        }
                        const beginDateTimeStr = (new Date(info.begin_time)).toLocalTimeString('iso').split(' ');
                        const beginDateStr = beginDateTimeStr[0];
                        const beginTimeStr = beginDateTimeStr[1];
                        let startTimeStr = info.start_time === 0 ? '' : (new Date(info.start_time)).toLocalTimeString('iso').split(' ')[1];
                        let transitionTimeStr = info.transition_time === 0 ? '' : (new Date(info.transition_time)).toLocalTimeString('iso').split(' ')[1];
                        let endTimeStr = info.end_time === 0 ? '' : (new Date(info.end_time)).toLocalTimeString('iso').split(' ')[1];
                        const startDeltaStr = info.start_time && info.begin_time ? ((info.start_time - info.begin_time) / 1000).toFixed(0) : '';
                        const transitionDeltaStr = info.transition_time && info.start_time ? ((info.transition_time - info.start_time) / 1000).toFixed(0) : '';
                        const endDeltaStr = info.end_time && info.transition_time ? ((info.end_time - info.transition_time) / 1000).toFixed(0) : '';
                        let numWorkers = info.contrib.summary.num_workers;
                        let numRegular = info.contrib.summary.num_regular_files;
                        let numChunks = info.contrib.summary.num_chunk_files;
                        let numChunkOverlaps = info.contrib.summary.num_chunk_overlap_files;
                        let numFiles = numRegular + numChunks + numChunkOverlaps;
                        let numRows = info.contrib.summary.num_rows;
                        let dataSize = info.contrib.summary.data_size_gb.toFixed(2);
                        return html + `
<tr>
  <th class="controls">
    <button type="button" class="activity btn btn-light btn-sm" id="${info.id}" title="Click to inspect transaction events log"><i class="bi bi-info-circle"></i></button>
    <button type="button" class="contrib btn btn-light btn-sm" id="${info.id}" title="Click to see contributions made in a scope of the transaction"><i class="bi bi-filetype-csv"></i></button>
  </th>
  <th class="right-aligned"><pre>${info.id}</pre></th>
  <td class="center-aligned ${transactionCssClass}"><pre>${info.state}</pre></td>
  <th class="right-aligned"><pre>${beginDateStr}</pre></th>
  <td class="right-aligned"><pre>${beginTimeStr}</pre></td>
  <th class="right-aligned"><pre>${startDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${startTimeStr}</pre></td>
  <th class="right-aligned"><pre>${transitionDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${transitionTimeStr}</pre></td>
  <th class="right-aligned"><pre>${endDeltaStr}</pre></th>
  <td class="right-aligned"><pre>${endTimeStr}</pre></td>
  <td class="right-aligned"><pre>${numWorkers}</pre></td>
  <td class="right-aligned"><pre>${numRegular}</pre></td>
  <td class="right-aligned"><pre>${numChunks}</pre></td>
  <td class="right-aligned"><pre>${numChunkOverlaps}</pre></td>
  <td class="right-aligned"><pre>${numFiles}</pre></td>
  <td class="right-aligned"><pre>${numRows}</pre></td>
  <td class="right-aligned"><pre>${dataSize}</pre></td>
</tr>`;
                    },
                    ''
                );
            }
            let tbody = this._table().children('tbody').html(html);
            tbody.find("button.activity").click(
                (e) => {
                    const transactionId = $(e.currentTarget).attr("id");
                    Fwk.find("Ingest", "Transaction Events Log").set_transaction(this._get_database_status(), this._databases, this._get_database(), this._transactions, transactionId);
                    Fwk.show("Ingest", "Transaction Events Log");
                }
            );
            tbody.find("button.contrib").click(
                (e) => {
                    const transactionId = $(e.currentTarget).attr("id");
                    Fwk.find("Ingest", "Contributions").set_transaction(this._get_database_status(), this._databases, this._get_database(), this._transactions, transactionId);
                    Fwk.show("Ingest", "Contributions");
                }
            );
        }
    }
    return IngestTransactions;
});
