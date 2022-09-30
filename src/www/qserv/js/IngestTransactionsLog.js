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

    CSSLoader.load('qserv/css/IngestTransactionsLog.css');

    class IngestTransactionsLog extends FwkApplication {

        known_operation_progress = {
            "begin add dir idx part": {
                "operation": "Adding director index partition",
                "progress": "BEGIN"
            },
            "end add dir idx part": {
                "operation": "Adding director index partition",
                "progress": "END"
            },
            "begin bld dir idx": {
                "operation": "Building director index",
                "progress": "BEGIN"
            },
            "progress bld dir idx": {
                "operation": "Building director index",
                "progress": "PROGRESS"
            },
            "end bld dir idx": {
                "operation": "Building director index",
                "progress": "END"
            },
            "begin del table part": {
                "operation": "Deleting data table partition",
                "progress": "BEGIN"
            },
            "progress del table part": {
                "operation": "Deleting data table partition",
                "progress": "PROGRESS"
            },
            "end del table part": {
                "operation": "Deleting data table partition",
                "progress": "END"
            },
            "begin del dir idx part": {
                "operation": "Deleting director index partition",
                "progress": "BEGIN"
            },
            "end del dir idx part": {
                "operation": "Deleting director index partition",
                "progress": "END"
            }
        };
        in_progress_states = ["STARTED", "IS_STARTING", "IS_FINISHING"];
        failed_states = ["START_FAILED", "FINISH_FAILED", "IS_ABORTING", "ABORT_FAILED", "ABORTED"];

        // These variables are used for optimization purposes to avoid complete refresh
        // of the events table. The variables is reset each time transaction identifier changes
        last_max_event_id = 0
        last_trans_id = 0

        constructor(name) {
            super(name);
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

        /// Set parameters of the transaction in the selectors and begin loading
        /// the contributions in the background.
        set_transaction(status, databases, database, transactions, trans_id) {
            this._init();
            this._set_database_status(status);
            this._set_databases(databases, database);
            this._set_transactions(transactions, trans_id);
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
            let html = `
<div class="form-row" id="fwk-ingest-transactions-log-controls">
  <div class="form-group col-md-1">
    <label for="database-status">Status:</label>
    <select id="database-status" class="form-control">
      <option value=""></option>
      <option value="INGESTING" selected>INGESTING</option>
      <option value="PUBLISHED">PUBLISHED</option>
    </select>
  </div>
  <div class="form-group col-md-3">
    <label for="database">Database:</label>
    <select id="database" class="form-control">
    </select>
  </div>
  <div class="form-group col-md-1">
    <label for="trans-id">Transaction Id:</label>
    <select id="trans-id" class="form-control"></select>
  </div>
  <div class="form-group col-md-1">
    <label for="trans-update-interval">Interval <i class="bi bi-arrow-repeat"></i></label>
    <select id="trans-update-interval" class="form-control">
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
  <div class="col" id="fwk-ingest-transactions-log-status">
    <div id="status">Loading...</div>
  </div>
</div>
<div class="row" id="fwk-ingest-transactions-log-info">
  <div class="col col-md-3">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">State</th><td style="text-align:left" id="state"><pre>Loading...</pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Created</th><td style="text-align:left"><pre id="begin_time">Loading...</pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Started</th><td style="text-align:left"><pre id="start_time">Loading...</pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Transitioned</th><td style="text-align:left"><pre id="transition_time">Loading...</pre></td>
        </tr>
        <tr>
          <th style="text-align:left" scope="row">Ended</th><td style="text-align:left"><pre id="end_time">Loading...</pre></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-1">
    <table class="table table-sm table-hover">
      <tbody>
        <tr>
          <th style="text-align:left" scope="row">Context</th>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="col col-md-8">
    <textarea style="" disabled id="context">
    Loading...
    </textarea>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-ingest-transactions-log">
      <thead class="thead-light">
        <tr>
          <th class="right-aligned">In State</th>
          <th class="right-aligned"><elem style="color:red;">&uarr;</elem>&nbsp;Event Time</th>
          <th class="right-aligned">Operation</th>
          <th class="right-aligned">Progress</th>
          <th class="right-aligned">Table</th>
          <th class="left-aligned">Error</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control").change(() => {
                this._load();
            });
            this._disable_controls(true);
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this.fwk_app_container.find('div#fwk-ingest-transactions-log-status > div#status');
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
        _get_database_status() { return this._form_control('select', 'database-status').val(); }
        _set_database_status(val) { this._form_control('select', 'database-status').val(val); }
        _get_database() { return this._form_control('select', 'database').val(); }
        _set_databases(databases, database=undefined) {
            // Keep the current selection after updating the selector in case if the
            // database belongs to this collection.
            const current_database = _.isUndefined(database) ? this._get_database() : database;
            let in_collection = false;
            this._form_control('select', 'database').html(
                _.reduce(databases, (html, name) => {
                    if (name === current_database) in_collection = true;
                    const selected = !html ? 'selected' : '';
                    return html + `<option value="${name}" ${selected}>${name}</option>`;
                }, '')
            );
            if (in_collection) {
                this._form_control('select', 'database').val(current_database);
            }
        }
        _disable_controls(disable) {
            this.fwk_app_container.find(".form-control").prop('disabled', disable);
        }
        _get_trans_id() { return this._form_control('select', 'trans-id').val(); }
        _set_transactions(transactions, trans_id=undefined) {
            // Keep the current selection after updating the selector in case if the
            // transaction belongs to this collection.
            const current_id = parseInt(_.isUndefined(trans_id) ? this._get_trans_id() : trans_id);
            let in_collection = false;
            this._form_control('select', 'trans-id').html(
                _.reduce(transactions, (html, id) => {
                    if (id === current_id) in_collection = true;
                    const selected = !html ? 'selected' : '';
                    return html + `<option value="${id}" ${selected}>${id}</option>`;
                }, '')
            );
            if (in_collection) {
                this._form_control('select', 'trans-id').val(current_id);
            }
        }
        _update_interval_sec() { return this._form_control('select', 'trans-update-interval').val(); }
        _trans_info(attr) {
            if (this._trans_info_obj === undefined) {
                this._trans_info_obj = this.fwk_app_container.find('div#fwk-ingest-transactions-log-info');
            }
            return this._trans_info_obj.find("#" + attr);
        }
        _set_trans_info_state(attr, val) {
            this._trans_info(attr).html(val);
        }
        _set_trans_info(attr, val) {
            this._trans_info(attr).text(val);
        }
        _table_events() {
            if (this._table_events_obj === undefined) {
                this._table_events_obj = this.fwk_app_container.find('table#fwk-ingest-transactions-log');
            }
            return this._table_events_obj;
        }
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            this._disable_controls(true);
            this._load_databases();
        }
        _load_databases() {
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    const status = this._get_database_status();
                    this._set_databases(
                        _.map(
                            _.filter(
                                data.config.databases,
                                function (info) { return (status === "") || (status === "PUBLISHED" && info.is_published) || (status === "INGESTING" && !info.is_published); }
                            ),
                            function (info) { return info.database; }
                        )
                    );
                    const current_database = this._get_database();
                    if (current_database) this._load_transactions(current_database);
                    else this._on_failure('No databases found in this status category');
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_transactions(current_database) {
            Fwk.web_service_GET(
                "/ingest/trans",
                {database: current_database, version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    let transactions = _.map(
                        _.sortBy(
                            data.databases[current_database].transactions,
                            function (info) { return info.id; }
                        ),
                        function (info) { return info.id; }
                    )
                    this._set_transactions(transactions);
                    const current_transaction = this._get_trans_id();
                    if (current_transaction) {
                        this._load_transaction_info(current_database, current_transaction);
                    } else {
                        this._on_failure('No transactions found in this database');
                    }
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _load_transaction_info(current_database, current_transaction) {
            Fwk.web_service_GET(
                "/ingest/trans/" + current_transaction,
                {include_context: 1, include_log: 1, version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._display(data.databases[current_database].transactions);
                    this._disable_controls(false);
                    Fwk.setLastUpdate(this._status());
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failure(msg); }
            );
        }
        _on_failure(msg) {
            this._status().html(`<span style="color:maroon">${msg}</span>`);
            this._status().removeClass('updating');
            this._table_events().children('tbody').html('');
            this._reset_info();
            this._disable_controls(false);
            this._loading = false;
        }
        _display(transactions) {
            console.log("IngestTransactionsLog._display databaseInfo", transactions);
            let tbody = this._table_events().children('tbody');
            let html = '';
            if (transactions.length === 0) {
                this._reset_info();
            } else {
                const info = transactions[0];
                if (info.id != this.last_trans_id) {
                    this.last_max_event_id = 0;
                    this.last_trans_id = info.id;
                }
                console.log("IngestTransactionsLog._display databaseInfo", transactions);
                this._set_trans_info_state("state", `<pre class="${this._state2class(info.state)}">${info.state}</pre>`);
                this._set_trans_info("begin_time", info.begin_time ? (new Date(info.begin_time)).toLocalTimeString('iso') : "");
                this._set_trans_info("start_time", info.start_time ? (new Date(info.start_time)).toLocalTimeString('iso') : "");
                this._set_trans_info("transition_time", info.transition_time ? (new Date(info.transition_time)).toLocalTimeString('iso') : "");
                this._set_trans_info("end_time", info.end_time ? (new Date(info.end_time)).toLocalTimeString('iso') : "");
                this._set_trans_info("context", _.isObject(info.context) && !_.isEmpty(info.context) ? JSON.stringify(info.context, null, 2) : "");
                const log = _.sortBy(info.log, "time").reverse();

                if (log.length) {
                    // Do not update the log unless it's newer than the previous sample
                    const latest_event = log[0];
                    if (latest_event.id > this.last_max_event_id) {
                        this.last_max_event_id = latest_event.id;
                        for (let i in log) {
                            const event = log[i];
                            const state = event.transaction_state;
                            const table = _.get(event.data, "table", "");
                            const op = _.has(this.known_operation_progress, event.name) ?
                            this.known_operation_progress[event.name] : {"operation": event.name, "progress": ""};
                            const progress = op.progress === "PROGRESS" ?
                                event.data.progress.complete + " / " + event.data.progress.total :
                                op.progress;
                            html += `
<tr id="${event.id}">
  <td class="right-aligned"><pre class="${this._state2class(state)}">${state}</pre></td>
  <td class="right-aligned"><pre>${(new Date(event.time)).toLocalTimeString('iso')}</pre></th>
  <td class="right-aligned">${op.operation}</th>
  <td class="right-aligned"><pre>${progress}</pre></th>
  <td class="right-aligned"><pre class="database_table" database="${info.database}" table="${table}">${table}</pre></th>
  <td class="left-aligned error">&nbsp;</th>
</tr>`;
                        }
                        tbody.html(html);
                        tbody.find("pre.database_table").click((e) => {
                            const elem = $(e.currentTarget);
                            const database = elem.attr("database");
                            const table = elem.attr("table");
                            Fwk.show("Replication", "Schema");
                            Fwk.current().loadSchema(database, table);
                        });
                        for (let i in log) {
                            const event = log[i];
                            const error = _.get(event.data, "error", undefined);
                            if (!_.isUndefined(error)) {
                                const errorStr = this._error2str(error);
                                if (errorStr) {
                                    let elem = tbody.find("tr#" + event.id + " td.error");
                                    elem.html('<textarea disabled></textarea>');
                                    elem.find('textarea').text(errorStr);
                                }
                            }
                        }
                    }
                } else {
                    tbody.html("");
                }
            }
        }
        _reset_info() {
            this._set_trans_info_state("state", "&nbsp;");
            this._set_trans_info("begin_time", "");
            this._set_trans_info("start_time", "");
            this._set_trans_info("transition_time", "");
            this._set_trans_info("end_time", "");
            this._set_trans_info("context", "");
            this._table_events().children('tbody').html("");
        }
        _state2class(state) {
            if (_.contains(this.in_progress_states, state)) return "trans-in-progress";
            else if (_.contains(this.failed_states, state)) return "trans-failed";
            return "trans-finished";
        }
        _error2str(error) {
            if (_.isObject(error) && !_.isEmpty(error)) {
                return JSON.stringify(error, null, 2);
            } else if (_.isString(error)) {
                return error;
            }
            return "";
        }
    }
    return IngestTransactionsLog;
});
