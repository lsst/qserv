define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
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
<div class="form-row">
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
            this._disable_databases(true);
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
        _get_database() { return this._form_control('select', 'ingest-database').val(); }
        _set_database(val) { this._form_control('select', 'ingest-database').val(val); }
        _set_databases(databases) {
            // To keep the current selection after updating the selector
            const current_database = this._get_database();
            let html = '';
            for (let i in databases) {
                const name = databases[i];
                const selected = i ? '' : 'selected'; 
                html += `<option value="${name}" ${selected}>${name}</option>`;
            }
            this._form_control('select', 'ingest-database').html(html);
            if (current_database) this._set_database(current_database);
        }
        _disable_databases(disable) {
            this._form_control('select', 'ingest-database').prop('disabled', disable);
        }
        _update_interval_sec() { return this._form_control('select', 'ingest-update-interval').val(); }

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
                        // Unpublished databases are shown on top of the selector's list
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
            this._database().html('');
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
            let databaseNumTransStarted = 0;
            let databaseNumTransFinished = 0;
            let databaseNumTransAborted = 0;
            let firstIngestTime = 0;
            let lastIngestTime = 0;
            let tableStats = {};
            let workerStats = {};
            for (let transactionIdx in databaseInfo.transactions) {

                let transactionInfo = databaseInfo.transactions[transactionIdx];
                switch (transactionInfo.state) {
                    case 'STARTED':  databaseNumTransStarted++;  break;
                    case 'FINISHED': databaseNumTransFinished++; break;
                    case 'ABORTED':  databaseNumTransAborted++;  break;
                }

                // For other summary data ignore transactions that has been aborted
                if (transactionInfo.state === 'ABORTED') continue;

                let summary = transactionInfo.contrib.summary;

                databaseDataSize += summary.data_size_gb;
                databaseNumRows  += summary.num_rows;
                for (let status in summary.num_files_by_status) {
                    databaseNumFilesByStatus[status] += summary.num_files_by_status[status];
                }
                let thisFirstContribTime = summary.first_contrib_begin;
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
                    let tableInfo = summary.table[table];
                    if (_.has(tableStats, table)) {
                        tableStats[table].data  += tableInfo.data_size_gb;
                        tableStats[table].rows  += tableInfo.num_rows;
                        tableStats[table].files += tableInfo.num_files;
                    } else {
                        tableStats[table] = {
                            'data':  tableInfo.data_size_gb,
                            'rows':  tableInfo.num_rows,
                            'files': tableInfo.num_files
                        };
                    }
                    if (_.has(tableInfo, 'overlap')) {
                        let tableOverlaps = table + '&nbsp;(overlaps)';
                        baseTableName[tableOverlaps] = table;
                        if (_.has(tableStats, tableOverlaps)) {
                            tableStats[tableOverlaps].data  += tableInfo.overlap.data_size_gb;
                            tableStats[tableOverlaps].rows  += tableInfo.overlap.num_rows;
                            tableStats[tableOverlaps].files += tableInfo.overlap.num_files;
                        } else {
                            tableStats[tableOverlaps] = {
                                'data':  tableInfo.overlap.data_size_gb,
                                'rows':  tableInfo.overlap.num_rows,
                                'files': tableInfo.overlap.num_files
                            };
                        }
                    }
                }

                // Collect per-worker-level stats
                for (let worker in summary.worker) {
                    let workerInfo = summary.worker[worker];
                    let numWorkerFiles = workerInfo.num_regular_files +
                                        workerInfo.num_chunk_files +
                                        workerInfo.num_chunk_overlap_files;
                    if (_.has(workerStats, worker)) {
                        workerStats[worker].data  += workerInfo.data_size_gb;
                        workerStats[worker].rows  += workerInfo.num_rows;
                        workerStats[worker].files += numWorkerFiles;
                    } else {
                        workerStats[worker] = {
                            'data':  workerInfo.data_size_gb,
                            'rows':  workerInfo.num_rows,
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
            let perfStr = 'n/a';
            if ((firstIngestTime > 0) && (lastIngestTime > 0) && (lastIngestTime > firstIngestTime)) {
                let perfGBps = databaseDataSize / ((lastIngestTime - firstIngestTime) / 1000.);
                perfStr = (1000. * perfGBps).toFixed(2) + ' MB/s';
            }
            const isPublishedStr = databaseInfo.is_published ? 'YES' : 'NO';
            html += `
    <div class="database">
      <div class="row block">
        <div class="col-md-auto">
          <table class="table table-sm table-hover">
            <tbody>
              <tr><th>Published</th><td class="right-aligned"><pre>${isPublishedStr}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Data [GB]</th><td class="right-aligned"><pre>${databaseDataSize.toFixed(2)}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Rows</th><td class="right-aligned"><pre>${databaseNumRows}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Alloc.chunks</th><td class="right-aligned"><pre>${databaseInfo.num_chunks}</pre></td><td>&nbsp;</td></tr>
              <tr><th>Transactions</th><td class="right-aligned"><pre>${databaseNumTransStarted}</pre></td><td><pre class="trans-started">STARTED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTransAborted}</pre></td><td><pre class="trans-aborted">ABORTED</pre></td</tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumTransFinished}</pre></td><td><pre class="trans-finished">FINISHED</pre></td></tr>
            </tbody>
          </table>
        </div>
        <div class="col-md-auto">
          <table class="table table-sm table-hover">
            <tbody>
              <tr><th>Contributions</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['IN_PROGRESS']}</pre></td><td><pre class="files-in-progress">IN_PROGRESS</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['CREATE_FAILED']}</pre></td><td><pre class="files-failed">CREATE_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['START_FAILED']}</pre></td><td><pre class="files-failed">START_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['READ_FAILED']}</pre></td><td><pre class="files-failed">READ_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['LOAD_FAILED']}</pre></td><td><pre class="files-failed">LOAD_FAILED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['CANCELLED']}</pre></td><td><pre class="files-failed">CANCELLED</pre></td></tr>
              <tr><th>&nbsp;</th><td class="right-aligned"><pre>${databaseNumFilesByStatus['FINISHED']}</pre></td><td><pre class="files-finished">FINISHED</pre></td></tr>
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
        <div class="col-md-auto perf">
          <h2><span class="perf-title">avg.perf:&nbsp;</span><span class="perf-value">${perfStr}</span></h2>
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
                <th class="right-aligned" style="border-top:none">data [GB]</th>
                <th class="right-aligned" style="border-top:none">rows</th>
                <th class="right-aligned" style="border-top:none">contribs</th>
              </tr>
            </thead>
            <thead>
              <tr>
                <th colspan="4" style="border-bottom:none">table</th>
              </tr>
            </thead>
            <tbody>`;
                for (let table in tableStats) {
                    html += `
                  <tr>
                    <td class="level-2"><pre class="database_table" database="${database}" table="${baseTableName[table]}">${table}</pre></td>
                    <td class="right-aligned"><pre>${tableStats[table].data.toFixed(2)}</pre></td>
                    <td class="right-aligned"><pre>${tableStats[table].rows}</pre></td>
                    <td class="right-aligned"><pre>${tableStats[table].files}</pre></td>
              </tr>`;
                }
                html += `
                </tbody>
                <thead>
                  <tr>
                    <th colspan="4" style="border-bottom:none">worker</th>
                  </tr>
                </thead>
            <tbody>`;
                for (let worker in workerStats) {
                    html += `
                  <tr>
                    <td class="level-2"><pre>${worker}</pre></td>
                    <td class="right-aligned"><pre>${workerStats[worker].data.toFixed(2)}</pre></td>
                    <td class="right-aligned"><pre>${workerStats[worker].rows}</pre></td>
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
