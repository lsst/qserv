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

    CSSLoader.load('qserv/css/QservWorkerQueries.css');

    class QservWorkerQueries extends FwkApplication {

        constructor(name) {
            super(name);
            this._queryId2Expanded = {};    // Store 'true' to allow persistent state for the expanded
                                            // queries between updates.
            this._id2query = {};            // Store query text for each identifier. The dictionary gets
                                            // updated at each refresh of the page.
            this._id2url = {};              // Store URL to the query blob for each identifier
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
<div class="row" id="fwk-qserv-queries-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 10)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-queries-form">&nbsp;</label>
        <button id="reset-queries-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-queries">
      <thead class="thead-light">
        <tr>
          <th class="sticky">worker</th>
          <th class="sticky">scheduler</th>
          <th class="sticky">#tasks</th>
          <th class="sticky">qid</th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-clipboard-fill"></i></th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-download"></i></th>
          <th class="sticky" style="text-align:center;"><i class="bi bi-info-circle-fill"></i></th>
          <th class="sticky">query</th>
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
            cont.find("button#reset-queries-form").click(() => {
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

        /**
         * Table for displaying queries that are being processed/scheduled at workers.
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-queries');
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
                "/replication/qserv/worker/status",
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
         * Display the queries
         */
        _display(data) {
            const queryCopyTitle = "Click to copy the query text to the clipboard.";
            const queryDownloadTitle = "Click to download the query text to your computer.";
            const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";
            const queryToggleTitle = "Click to toggle query formatting.";
            const queryStyle = "color:#4d4dff;";
            let html = '';
            this._id2query = {};
            for (let id in this._id2url) {
                URL.revokeObjectURL(this._id2url[id]);
            }
            for (let worker in data) {
                if (!data[worker].success) {
                    html += `
<tr>
  <th class="table-warning">${worker}</th>
  <th class="table-secondary">&nbsp;</th>
  <th class="table-secondary">&nbsp;</th>
  <th class="table-secondary">&nbsp;</th>
  <th class="table-secondary">&nbsp;</th>
</tr>`;
                } else {
                    let queries = data[worker].queries;
                    let schedulers = data[worker].info.processor.queries.blend_scheduler.schedulers;
                    let numQueries = 0;
                    let numSchedulers = 0;
                    let htmlQueries   = '';
                    for (let i in schedulers) {
                        let scheduler = schedulers[i];
                        let numSchedulerQueries = 0;
                        let htmlSchedulerQueries = '';
                        for (let j in scheduler.query_id_to_count) {
                            numQueries++;
                            numSchedulerQueries++;
                            const queryId  = scheduler.query_id_to_count[j][0];
                            const numTasks = scheduler.query_id_to_count[j][1];
                            this._id2query[queryId] = queries[queryId].query;
                            this._id2url[queryId] = URL.createObjectURL(new Blob([queries[queryId].query], {type: "text/plain"}));
                            const expanded = (queryId in this._queryId2Expanded) && this._queryId2Expanded[queryId];
                            htmlSchedulerQueries += `
<tr id="${queryId}">
  <td><pre>${numTasks}</pre></td>
  <th><pre>${queryId}</pre></th>
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-dark btn-sm copy-query" style="height:20px; margin:0px;" title="${queryCopyTitle}"></button>
  </td>
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <a class="btn btn-outline-dark btn-sm" style="height:20px; margin:0px;" title="${queryDownloadTitle}" href="${this._id2url[queryId]}" download></a>
  </td>
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
  <td class="query_toggler" title="${queryToggleTitle}"><pre class="query" style="${queryStyle}">` + this._query2text(queryId, expanded) + `<pre></td>
</tr>`;
                        }
                        if (numSchedulerQueries > 0) {
                            numSchedulers++;
                            htmlQueries += `
<tr>
  <td rowspan="${numSchedulerQueries+1}"><pre>${scheduler.name.substring("Sched".length)}</pre></td>
</tr>` + htmlSchedulerQueries;
                        }
                    }
                    html += `
<tr>
  <th rowspan="${numSchedulers+numQueries+1}">${worker}</th>
</tr>` + htmlQueries;
                }
            }
            let that = this;
            let copyQueryToClipboard = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                let query = that._id2query[queryId];
                navigator.clipboard.writeText(query,
                    () => {},
                    () => { alert("Failed to write the query to the clipboard. Please copy the text manually: " + query); }
                );
            };
            let displayQuery  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                Fwk.find("Status", "Query Inspector").set_query_id(queryId);
                Fwk.show("Status", "Query Inspector");
            };
            let toggleQueryDisplay = function(e) {
                let td = $(e.currentTarget);
                let pre = td.find("pre.query");
                const queryId = td.parent().attr("id");
                const expanded = !((queryId in that._queryId2Expanded) && that._queryId2Expanded[queryId]);
                pre.text(that._query2text(queryId, expanded));
                that._queryId2Expanded[queryId] = expanded;
            };
            let tbodyQueries = this._table().children('tbody').html(html);
            tbodyQueries.find("button.copy-query").click(copyQueryToClipboard);
            tbodyQueries.find("button.inspect-query").click(displayQuery);
            tbodyQueries.find("td.query_toggler").click(toggleQueryDisplay);
        }
        _query2text(queryId, expanded) {
            return Common.query2text(this._id2query[queryId], expanded);
        }
    }
    return QservWorkerQueries;
});
