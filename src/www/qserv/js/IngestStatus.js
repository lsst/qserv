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

    CSSLoader.load('qserv/css/IngestStatus.css');

    class IngestStatus extends FwkApplication {

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

        /// The first time initialization of the page's layout
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            this._prevTimestamp = 0;
            let html = `
<div class="form-row" id="fwk-ingest-status-controls">
  <div class="form-group col-md-1">
    <label for="ingest-database-status">Status:</label>
    <select id="ingest-database-status" class="form-control form-control-view">
      <option value=""></option>
      <option value="INGESTING" selected>INGESTING</option>
      <option value="PUBLISHED">PUBLISHED</option>
    </select>
  </div>
  <div class="form-group col-md-3">
    <label for="ingest-database">Database:</label>
    <select id="ingest-database" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-1">
    <label for="ingest-update-interval"><i class="bi bi-arrow-repeat"></i> interval:</label>
    <select id="ingest-update-interval" class="form-control form-control-view">
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
  <div class="col" id="fwk-ingest-status">
    <div id="status">Loading...</div>
    <div id="database"></div>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-view").change(() => {
                this._load();
            });
            this._disable_selectors(true);
        }
        
        /// @returns JQuery object displaying the last update time of the page
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this.fwk_app_container.find('div#fwk-ingest-status').children('div#status');
            }
            return this._status_obj;
        }

        /// @returns JQuery object for a container where the database status will be displayed
        _database() {
            if (this._database_obj === undefined) {
                this._database_obj = this.fwk_app_container.find('div#fwk-ingest-status').children('div#database');
            }
            return this._database_obj;
        }

        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _get_database_status() { return this._form_control('select', 'ingest-database-status').val(); }
        _get_database() { return this._form_control('select', 'ingest-database').val(); }
        _set_database(val) { this._form_control('select', 'ingest-database').val(val); }
        _set_databases(databases) {
            // Keep the current selection after updating the selector in case if the
            // database belongs to this collection.
            const current_database = this._get_database();
            let in_collection = false;
            this._form_control('select', 'ingest-database').html(
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
        _update_interval_sec() { return this._form_control('select', 'ingest-update-interval').val(); }

        /// Load data from a web service then render it to the application's page.
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
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failure(data.error);
                        return;
                    }
                    this._set_databases(
                        _.map(
                            _.filter(
                                data.config.databases,
                                function (info) {
                                    return (status === "") ||
                                           ((status === "PUBLISHED") && info.is_published) ||
                                           ((status === "INGESTING") && !info.is_published);
                                }
                            ),
                            function (info) { return info.database; }
                        )
                    );
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
                {database: current_database, contrib: 1, contrib_long: 0, version: Common.RestAPIVersion},
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
            this._database().html('');
            this._disable_selectors(false);
            this._status().removeClass('updating');
            this._loading = false;
        }

        /**
         * Render the data received from a server
         * @param {Object} databaseInfo  transactions and other relevant info for the select database
         */
        _display(databaseInfo) {

            let html = '';

            const database = this._get_database();

            // Translation map to allow back-referencing overlap tables to their base ones
            let baseTableName = {};

            // Transaction timestamps are computed below based on the above made sorting
            // of transactions in the DESC order by the begin time.
            let firstTransBeginStr = 'n/d';
            let firstTransBeginAgoStr = '&nbsp;';
            let lastTransBeginStr = 'n/d';
            let lastTransBeginAgoStr = '&nbsp;';
            if (databaseInfo.transactions.length > 0) {

                let firstTransBegin = databaseInfo.transactions[databaseInfo.transactions.length-1].begin_time;
                firstTransBeginStr = (new Date(firstTransBegin)).toLocalTimeString('short');
                firstTransBeginAgoStr = IngestStatus.timeAgo(firstTransBegin);

                let lastTransBegin = databaseInfo.transactions[0].begin_time;
                lastTransBeginStr = (new Date(lastTransBegin)).toLocalTimeString('short');
                lastTransBeginAgoStr = IngestStatus.timeAgo(lastTransBegin);
            }

            // Other counters are computed based on what's found in the transaction
            // summary sections.
            let databaseDataSize = 0;
            let databaseNumRows = 0;
            let databaseNumRowsLoaded = 0;
            let databaseNumFailedRetries = 0;
            let databaseNumWarnings = 0;
            let databaseNumFilesByStatus = {
                'IN_PROGRESS': 0,
                'CREATE_FAILED': 0,
                'START_FAILED': 0,
                'READ_FAILED': 0,
                'LOAD_FAILED': 0,
                'CANCELLED': 0,
                'EXPIRED': 0,
                'FINISHED': 0
            };
            let databaseNumTrans = {
              "IS_STARTING" : 0,
              "STARTED" : 0,
              "IS_FINISHING" : 0,
              "IS_ABORTING" : 0,
              "FINISHED" : 0,
              "START_FAILED" : 0,
              "FINISH_FAILED" : 0,
              "ABORT_FAILED" : 0,
              "ABORTED" : 0
            };


            let firstIngestTime = 0;
            let lastIngestTime = 0;
            let tableStats = {};
            let workerStats = {};
            for (let transactionIdx in databaseInfo.transactions) {
                let transactionInfo = databaseInfo.transactions[transactionIdx];
                databaseNumTrans[transactionInfo.state]++;

                // For other summary data ignore transactions that have been aborted, or failed in
                // other ways.
                if (!_.contains(['IS_STARTING', 'STARTED', 'IS_FINISHING', 'FINISHED'], transactionInfo.state)) continue;

                const summary = transactionInfo.contrib.summary;
                databaseDataSize += summary.data_size_gb;
                databaseNumRows  += summary.num_rows;
                databaseNumRowsLoaded  += summary.num_rows_loaded;
                databaseNumFailedRetries  += summary.num_failed_retries;
                databaseNumWarnings  += summary.num_warnings;
                for (let status in summary.num_files_by_status) {
                    databaseNumFilesByStatus[status] += summary.num_files_by_status[status];
                }
                const thisFirstContribTime = summary.first_contrib_begin;
                if (thisFirstContribTime > 0) {
                    firstIngestTime = firstIngestTime === 0 ?
                        thisFirstContribTime : Math.min(firstIngestTime, thisFirstContribTime);
                }
                lastIngestTime = Math.max(lastIngestTime, summary.last_contrib_end);

                // Collect per-table-level stats
                for (let table in summary.table) {
                    baseTableName[table] = table;
                    // This object has data for both chunk and chunk overlaps. So we need
                    // to absorbe both.
                    const tableInfo = summary.table[table];
                    if (_.has(tableStats, table)) {
                        tableStats[table].data  += tableInfo.data_size_gb;
                        tableStats[table].num_rows  += tableInfo.num_rows;
                        tableStats[table].num_rows_loaded  += tableInfo.num_rows_loaded;
                        tableStats[table].num_failed_retries  += tableInfo.num_failed_retries;
                        tableStats[table].num_warnings  += tableInfo.num_warnings;
                        tableStats[table].files += tableInfo.num_files;
                    } else {
                        tableStats[table] = {
                            'data': tableInfo.data_size_gb,
                            'num_rows': tableInfo.num_rows,
                            'num_rows_loaded': tableInfo.num_rows_loaded,
                            'num_failed_retries': tableInfo.num_failed_retries,
                            'num_warnings': tableInfo.num_warnings,
                            'files': tableInfo.num_files
                        };
                    }
                    if (_.has(tableInfo, 'overlap')) {
                        const tableOverlaps = table + '&nbsp;(overlaps)';
                        baseTableName[tableOverlaps] = table;
                        if (_.has(tableStats, tableOverlaps)) {
                            tableStats[tableOverlaps].data  += tableInfo.overlap.data_size_gb;
                            tableStats[tableOverlaps].num_rows  += tableInfo.overlap.num_rows;
                            tableStats[tableOverlaps].num_rows_loaded  += tableInfo.overlap.num_rows_loaded;
                            tableStats[tableOverlaps].num_failed_retries  += tableInfo.overlap.num_failed_retries;
                            tableStats[tableOverlaps].num_warnings  += tableInfo.overlap.num_warnings;
                            tableStats[tableOverlaps].files += tableInfo.overlap.num_files;
                        } else {
                            tableStats[tableOverlaps] = {
                                'data':  tableInfo.overlap.data_size_gb,
                                'num_rows':  tableInfo.overlap.num_rows,
                                'num_rows_loaded':  tableInfo.overlap.num_rows_loaded,
                                'num_failed_retries':  tableInfo.overlap.num_failed_retries,
                                'num_warnings':  tableInfo.overlap.num_warnings,
                                'files': tableInfo.overlap.num_files
                            };
                        }
                    }
                }

                // Collect per-worker-level stats
                for (let worker in summary.worker) {
                    const workerInfo = summary.worker[worker];
                    const numWorkerFiles = workerInfo.num_regular_files +
                                           workerInfo.num_chunk_files +
                                           workerInfo.num_chunk_overlap_files;
                    if (_.has(workerStats, worker)) {
                        workerStats[worker].data  += workerInfo.data_size_gb;
                        workerStats[worker].num_rows  += workerInfo.num_rows;
                        workerStats[worker].num_rows_loaded  += workerInfo.num_rows_loaded;
                        workerStats[worker].num_failed_retries  += workerInfo.num_failed_retries;
                        workerStats[worker].num_warnings  += workerInfo.num_warnings;
                        workerStats[worker].files += numWorkerFiles;
                    } else {
                        workerStats[worker] = {
                            'data':  workerInfo.data_size_gb,
                            'num_rows':  workerInfo.num_rows,
                            'num_rows_loaded':  workerInfo.num_rows_loaded,
                            'num_failed_retries':  workerInfo.num_failed_retries,
                            'num_warnings':  workerInfo.num_warnings,
                            'files': numWorkerFiles
                        };
                    }
                }
            }
            let firstIngestTimeStr = 'n/d';
            let firstIngestTimeAgoStr = '&nbsp;';
            if (firstIngestTime > 0) {
                firstIngestTimeStr = (new Date(firstIngestTime)).toLocalTimeString('short');
                firstIngestTimeAgoStr = IngestStatus.timeAgo(firstIngestTime);
            }
            let lastIngestTimeStr = 'n/d';
            let lastIngestTimeAgoStr = '&nbsp;';
            if (lastIngestTime > 0) {
                lastIngestTimeStr = (new Date(lastIngestTime)).toLocalTimeString('short');
                lastIngestTimeAgoStr = IngestStatus.timeAgo(lastIngestTime);
            }
            let perfStr = 0;
            if ((firstIngestTime > 0) && (lastIngestTime > 0) && (lastIngestTime > firstIngestTime)) {
                let perfGBps = databaseDataSize / ((lastIngestTime - firstIngestTime) / 1000.);
                perfStr = (1000. * perfGBps).toFixed(2);
            }
            const catalogStatus = databaseInfo.is_published ? 'PUBLISHED' : 'INGESTING';
            let attentionCssClass4rows = databaseNumRowsLoaded === databaseNumRows ? '' : 'table-danger';
            let attentionCssClass4retries = databaseNumFailedRetries === 0 ? '' : 'table-warning';
            let attentionCssClass4warnings = databaseNumWarnings === 0 ? '' : 'table-danger';
            html += `
    <div class="database">
      <div class="row block">
        <div class="col-md-auto">
          <table class="table table-sm table-hover">
            <tbody>
              <tr><th>Status</th><td class="right-aligned"><pre>${catalogStatus}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Chunks</th><td class="right-aligned"><pre>${databaseInfo.num_chunks}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Rows</th><td class="right-aligned"><pre>${databaseNumRows}</pre></td><td>&nbsp;</td></tr>
              <tr class="${attentionCssClass4rows}"><th>Rows&nbsp;loaded</th><td class="right-aligned"><pre>${databaseNumRowsLoaded}</pre></td><td>&nbsp;</td></tr>
              <tr class="${attentionCssClass4retries}"><th>Failed retries</th><td class="right-aligned"><pre>${databaseNumFailedRetries}</pre></td><td>&nbsp;</td></tr>
              <tr class="${attentionCssClass4warnings}"><th>Warnings</th><td class="right-aligned"><pre>${databaseNumWarnings}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Data [GB]</th><td class="right-aligned"><pre>${databaseDataSize.toFixed(2)}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Performance [MB/s]:</th><td class="right-aligned"><pre>${perfStr}</pre></td><td>&nbsp;</td></tr>
            </tbody>
          </table>
        </div>
        <div class="col-md-auto">
          <table class="table table-sm table-hover">
            <tbody>
            <tr><th>Transactions</th><td class="right-aligned"><pre>${databaseNumTrans['IS_STARTING']}</pre></td><td><pre class="trans-started">IS_STARTING</pre></td></tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['STARTED']}</pre></td><td><pre class="trans-started">STARTED</pre></td</tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['IS_FINISHING']}</pre></td><td><pre class="trans-started">IS_FINISHING</pre></td</tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['FINISHED']}</pre></td><td><pre class="trans-finished">FINISHED</pre></td></tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['START_FAILED']}</pre></td><td><pre class="trans-aborted">START_FAILED</pre></td</tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['FINISH_FAILED']}</pre></td><td><pre class="trans-aborted">FINISH_FAILED</pre></td</tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['IS_ABORTING']}</pre></td><td><pre class="trans-aborted">IS_ABORTING</pre></td</tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['ABORT_FAILED']}</pre></td><td><pre class="trans-aborted">ABORT_FAILED</pre></td</tr>
            <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTrans['ABORTED']}</pre></td><td><pre class="trans-aborted">ABORTED</pre></td</tr>
          </tbody>
          </table>
        </div>
        <div class="col-md-auto">
          <table class="table table-sm table-hover">
            <tbody>
              <tr><th>Contributions</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['IN_PROGRESS']}</pre></td><td><pre class="files-in-progress">IN_PROGRESS</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['FINISHED']}</pre></td><td><pre class="files-finished">FINISHED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['CREATE_FAILED']}</pre></td><td><pre class="files-failed">CREATE_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['START_FAILED']}</pre></td><td><pre class="files-failed">START_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['READ_FAILED']}</pre></td><td><pre class="files-failed">READ_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['LOAD_FAILED']}</pre></td><td><pre class="files-failed">LOAD_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['CANCELLED']}</pre></td><td><pre class="files-failed">CANCELLED</pre></td></tr>
            </tbody>
          </table>
        </div>
        <div class="col-md-auto">
          <table class="table table-sm table-hover">
            <tbody>
              <tr><th>First trans</th><td class="right-aligned"><pre class="object">${firstTransBeginStr}</pre></td><td class="right-aligned"><pre class="comment>">${firstTransBeginAgoStr}</pre></td></tr>
              <tr><th>Last trans</th><td class="right-aligned"><pre class="object">${lastTransBeginStr}</pre></td><td class="right-aligned"><pre class="comment>">${lastTransBeginAgoStr}</pre></td></tr>
              <tr><th>First contrib</th><td class="right-aligned"><pre class="object">${firstIngestTimeStr}</pre></td><td class="right-aligned"><pre class="comment>">${firstIngestTimeAgoStr}</pre></td></tr>
              <tr><th>Last contrib</th><td class="right-aligned"><pre class="object">${lastIngestTimeStr}</pre></td><td class="right-aligned"><pre class="comment>">${lastIngestTimeAgoStr}</pre></td></tr>
            </tbody>
          </table>
        </div>
      </div>`;

            // Skip displaying the empty table of tables and workers if no stats is available for both.
            if (!_.isEmpty(tableStats) && !_.isEmpty(workerStats)) {

                html += `
      <div class="row block">
        <div class="col">
          <table class="table table-sm table-hover">
            <thead class="thead-light">
              <tr>
                <th style="border-top:none">&nbsp;</th>
                <th class="right-aligned" style="border-top:none">Data [GB]</th>
                <th class="right-aligned" style="border-top:none">Rows</th>
                <th class="right-aligned" style="border-top:none">Rows loaded</th>
                <th class="right-aligned" style="border-top:none">Failed&nbsp;retries</th>
                <th class="right-aligned" style="border-top:none">Warnings</th>
                <th class="right-aligned" style="border-top:none">Contribs</th>
              </tr>
            </thead>
            <thead>
              <tr>
                <th colspan="4" style="border-bottom:none">Table</th>
              </tr>
            </thead>
            <tbody>`;
                for (let table in tableStats) {
                    let attentionCssClass4rows = tableStats[table].num_rows_loaded === tableStats[table].num_rows ? '' : 'table-danger';
                    let attentionCssClass4retries = tableStats[table].num_failed_retries === 0 ? '' : 'table-warning';
                    let attentionCssClass4warnings = tableStats[table].num_warnings === 0 ? '' : 'table-danger';
                    html += `
                  <tr>
                    <td class="level-2"><pre class="database_table" database="${database}" table="${baseTableName[table]}">${table}</pre></td>
                    <td class="right-aligned"><pre>${tableStats[table].data.toFixed(2)}</pre></td>
                    <td class="right-aligned"><pre>${tableStats[table].num_rows}</pre></td>
                    <td class="right-aligned ${attentionCssClass4rows}"><pre>${tableStats[table].num_rows_loaded}</pre></td>
                    <td class="right-aligned ${attentionCssClass4retries}"><pre>${tableStats[table].num_failed_retries}</pre></td>
                    <td class="right-aligned ${attentionCssClass4warnings}"><pre>${tableStats[table].num_warnings}</pre></td>
                    <td class="right-aligned"><pre>${tableStats[table].files}</pre></td>
              </tr>`;
                }
                html += `
                </tbody>
                <thead>
                  <tr>
                    <th colspan="4" style="border-bottom:none">Worker</th>
                  </tr>
                </thead>
            <tbody>`;
                for (let worker in workerStats) {
                    let attentionCssClass4rows = workerStats[worker].num_rows_loaded === workerStats[worker].num_rows ? '' : 'table-danger';
                    let attentionCssClass4retries = workerStats[worker].num_failed_retries === 0 ? '' : 'table-warning';
                    let attentionCssClass4warnings = workerStats[worker].num_warnings === 0 ? '' : 'table-danger';
                    html += `
                  <tr>
                    <td class="level-2"><pre>${worker}</pre></td>
                    <td class="right-aligned"><pre>${workerStats[worker].data.toFixed(2)}</pre></td>
                    <td class="right-aligned"><pre>${workerStats[worker].num_rows}</pre></td>
                    <td class="right-aligned ${attentionCssClass4rows}"><pre>${workerStats[worker].num_rows_loaded}</pre></td>
                    <td class="right-aligned ${attentionCssClass4retries}"><pre>${workerStats[worker].num_failed_retries}</pre></td>
                    <td class="right-aligned ${attentionCssClass4warnings}"><pre>${workerStats[worker].num_warnings}</pre></td>
                    <td class="right-aligned"><pre>${workerStats[worker].files}</pre></td>
                  </tr>`;
                }
            }
            html += `
            </tbody>
          </table>
        </div>
      </div>
    </div>`;
            this._database().html(html).find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
        }

        static timeAgo(timestamp) {
            let ivalSec = Fwk.now().sec - Math.floor(timestamp / 1000);
            if (ivalSec <         0) return '&lt;error&gt;';
            if (ivalSec <        60) return 'just now';
            if (ivalSec <      3600) return Math.floor(ivalSec / 60) + ' mins ago';
            if (ivalSec < 24 * 3600) return Math.floor(ivalSec / 3600) + ' hrs ago';
            return Math.floor(ivalSec / (24 * 3600)) + ' days ago';
        }
    }
    return IngestStatus;
});
