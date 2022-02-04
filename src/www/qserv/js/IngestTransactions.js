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
        }

        /// @see FwkApplication.fwk_app_on_show
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /// @see FwkApplication.fwk_app_on_hide
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
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

            /* <span style="color:maroon">&sum;</span>&nbsp; */

            let html = `
<div class="form-row">
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
          <th rowspan="2" class="right-aligned">id</th>
          <th rowspan="2" class="center-aligned">state</th>
          <th rowspan="2">begin time</th>
          <th rowspan="2">commit/abort time</th>
          <th colspan="7" class="center-aligned">transaction contributions</th>
        </tr>
        <tr>
          <th class="right-aligned">workers</th>
          <th class="right-aligned">reg.</th>
          <th class="right-aligned">chunks</th>
          <th class="right-aligned">overlaps</th>
          <th class="right-aligned">files</th>
          <th class="right-aligned">rows</th>
          <th class="right-aligned">data [GB]</th>
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
            this._disable_databases(true);
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _get_database() { return this._form_control('select', 'trans-database').val(); }
        _set_database(val) { this._form_control('select', 'trans-database').val(val); }
        _set_databases(databases) {
            // Keep the current selection after updating the selector
            const current_database = this._get_database();
            this._form_control('select', 'trans-database').html(
                _.reduce(
                    databases,
                    (html, name) => {
                        const selected = !html ? 'selected' : ''; 
                        return html + `<option value="${name}" ${selected}>${name}</option>`;
                    },
                    ''
                )
            );
            if (current_database) this._set_database(current_database);
        }
        _disable_databases(disable) {
            this._form_control('select', 'trans-database').prop('disabled', disable);
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

        /// Load data from a web service then render it to the application's page.
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._status().addClass('updating');
            this._disable_databases(true);
            this._load_databases();
        }
        _load_databases() {
            Fwk.web_service_GET(
                "/replication/config",
                {},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._set_databases(_.map(
                        _.sortBy(data.config.databases, function (info) { return info.is_published; }),
                        function (info) { return info.database; }
                    ));
                    this._load_transactions();
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_transactions() {
            const current_database = this._get_database();
            if (!current_database) {
                this._on_failure("No databases exist yet in this instance of Qserv");
                return;
            }
            Fwk.web_service_GET(
                "/ingest/trans",
                {database: current_database, contrib: 1, contrib_long: 0},
                (data) => {
                    if (!data.success) {
                        this._on_failure(msg);
                        return;
                    }
                    this._display(data.databases[current_database]);
                    this._disable_databases(false);
                    Fwk.setLastUpdate(this._status());
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _on_failure(msg) {
            this._status().html(`<span style="color:maroon">${msg}</span>`);
            this._table().html('');
            this._status().removeClass('updating');
            this._loading = false;
        }

        /**
         * Render the data received from a server
         * @param {Object} databases  transactions and other relevant info for the select database
         */
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
                // The collection of all transaction identifiers in a scope of the current database
                // will be passed to the conbtributions page when a click on a transaction is made.
                this._transactions = [];
                html += _.reduce(
                    // Transactions are shown sorted in the ASC order
                    _.sortBy(databaseInfo.transactions, function (info) { return info.id; }),
                    (html, transactionInfo) => {
                        this._transactions.push(transactionInfo.id);
                        let transactionCssClass = 'bg-white';
                        switch (transactionInfo.state) {
                            case 'STARTED':  transactionCssClass = 'bg-transparent'; break;
                            case 'FINISHED': transactionCssClass = 'alert alert-success'; break;
                            case 'ABORTED':  transactionCssClass = 'alert alert-danger'; break;
                        }
                        let beginTimeStr = (new Date(transactionInfo.begin_time)).toLocalTimeString('iso');
                        let endTimeStr = transactionInfo.end_time === 0 ? '' : (new Date(transactionInfo.end_time)).toLocalTimeString('iso');
                        let numWorkers = transactionInfo.contrib.summary.num_workers;
                        let numRegular = transactionInfo.contrib.summary.num_regular_files;
                        let numChunks = transactionInfo.contrib.summary.num_chunk_files;
                        let numChunkOverlaps = transactionInfo.contrib.summary.num_chunk_overlap_files;
                        let numFiles = numRegular + numChunks + numChunkOverlaps;
                        let numRows = transactionInfo.contrib.summary.num_rows;
                        let dataSize = transactionInfo.contrib.summary.data_size_gb.toFixed(2);
                        return html + `
<tr class="transaction" id="${transactionInfo.id}">
  <th class="right-aligned"><pre>${transactionInfo.id}</pre></th>
  <td class="center-aligned ${transactionCssClass}"><pre>${transactionInfo.state}</pre></th>
  <td class="right-aligned"><pre>${beginTimeStr}</pre></th>
  <td class="right-aligned"><pre>${endTimeStr}</pre></th>
  <td class="right-aligned"><pre>${numWorkers}</pre></th>
  <td class="right-aligned"><pre>${numRegular}</pre></th>
  <td class="right-aligned"><pre>${numChunks}</pre></th>
  <td class="right-aligned"><pre>${numChunkOverlaps}</pre></th>
  <td class="right-aligned"><pre>${numFiles}</pre></th>
  <td class="right-aligned"><pre>${numRows}</pre></th>
  <td class="right-aligned"><pre>${dataSize}</pre></th>
</tr>`;
                    },
                    ''
                );
            }
            this._table().children('tbody').html(html).children("tr.transaction").click(
                (e) => {
                    const tr = $(e.currentTarget);
                    const transactionId = tr.attr("id");
                    Fwk.show("Ingest", "Contributions");
                    Fwk.current().loadTransaction(this._get_database(), this._transactions, transactionId);
                }
            );
        }
    }
    return IngestTransactions;
});
