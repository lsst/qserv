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

    CSSLoader.load('qserv/css/QservWorkerHistograms.css');

    class QservWorkerHistograms extends FwkApplication {

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
        static _histogram_names = [
            'timeRunningPerTask',
            'timeSubchunkPerTask',
            'timeTransmittingPerTask',
            'sizePerTask',
            'rowsPerTask'
        ];
        static _table_head(histogram) {
            if (_.isUndefined(histogram)) {
                return `
<tr>
  <th class="sticky">worker</th>
</tr>`;
            }
            let html = `
<tr>
  <th class="sticky">worker</th>
  <th class="sticky" style="text-align:right;">QID</th>
  <th class="sticky" style="text-align:center;"><i class="bi bi-clipboard-fill"></i></th>
  <th class="sticky" style="text-align:right;">total</th>
  <th class="sticky" style="text-align:right;">totalCount</th>
  <th class="sticky" style="text-align:right;">avg</th>`;
            for (let i in histogram.buckets) {
                let bucket = histogram.buckets[i];
                html += `
<th class="sticky" style="text-align:right;">${i == 0 ? "&le;&nbsp;" : ""}${bucket.maxVal}</th>`;
            }
            html += `
</tr>`;
            return html;
        }
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="row" id="fwk-qserv-histograms-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="histogram-name">Histogram:</label>
        <select id="histogram-name" class="form-control form-control-selector">`;
            for (let i in QservWorkerHistograms._histogram_names) {
                const name = QservWorkerHistograms._histogram_names[i];
                html += `
          <option value="${name}">${name}</option>`;
            }
            html += `
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
        <label for="reset-histograms-form">&nbsp;</label>
        <button id="reset-histograms-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-histograms">
      <thead class="thead-light">
        ${QservWorkerHistograms._table_head()}
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
            cont.find("button#reset-histograms-form").click(() => {
                this._set_histogram_name(QservWorkerHistograms._histogram_names[0]);
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
        _histogram_name() { return this._form_control('select', 'histogram-name').val(); }
        _set_histogram_name(val) { this._form_control('select', 'histogram-name').val(val); }

        /**
         * Table for displaying histograms that are being produced at workers.
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-histograms');
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
         * Display histograms
         */
        _display(data) {
            const queryInspectTitle = "Click to see detailed info (progress, messages, etc.) on the query.";
            const histogram_name = this._histogram_name();
            let thead_html = QservWorkerHistograms._table_head();
            let tbody_html = '';
            for (let worker in data) {
                if (!data[worker].success || _.isUndefined(data[worker].info.processor) ||
                                             _.isUndefined(data[worker].info.processor.queries) ||
                                             _.isUndefined(data[worker].info.processor.queries.query_stats)) {
                    continue; 
                }
                let query_stats = data[worker].info.processor.queries.query_stats;
                if (_.isEmpty(query_stats)) continue;
                let rowspan = 1;
                let html   = '';
                for (let queryId in query_stats) {
                    if (!_.has(query_stats[queryId], "histograms")) continue;
                    let histograms = query_stats[queryId].histograms;
                    if (!_.has(histograms, histogram_name)) continue;
                    let histogram = histograms[histogram_name];
                    if (_.isEmpty(html)) {
                        thead_html = QservWorkerHistograms._table_head(histogram);
                    }
                    html += `
<tr id="${queryId}">
  <td style="text-align:right;"><pre>${queryId}</pre></td>
  <td style="text-align:center; padding-top:0; padding-bottom:0">
    <button class="btn btn-outline-info btn-sm inspect-query" style="height:20px; margin:0px;" title="${queryInspectTitle}"></button>
  </td>
  <td style="text-align:right;"><pre>${histogram.total.toFixed(3)}</pre></td>
  <td style="text-align:right;"><pre>${histogram.totalCount}</pre></td>
  <th style="text-align:right;"><pre>${histogram.avg.toFixed(3)}</pre></th>`;
                    for (let i in histogram.buckets) {
                        let bucket = histogram.buckets[i];
                        html += `
  <td style="text-align:right;"><pre>${bucket.count}</pre></td>`;
                    }
                    html += `
</tr>`;
rowspan++;
                }
                tbody_html += `
<tr>
  <th rowspan="${rowspan}">${worker}</th>
</tr>` + html;
            }
            this._table().children('thead').html(thead_html);
            let tbody = this._table().children('tbody').html(tbody_html);
            let displayQuery  = function(e) {
                let button = $(e.currentTarget);
                let queryId = button.parent().parent().attr("id");
                Fwk.find("Status", "Query Inspector").set_query_id(queryId);
                Fwk.show("Status", "Query Inspector");
            };
            tbody.find("button.inspect-query").click(displayQuery);
        }
    }
    return QservWorkerHistograms;
});
