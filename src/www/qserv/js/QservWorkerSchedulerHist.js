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

    CSSLoader.load('qserv/css/QservWorkerSchedulerHist.css');

    class QservWorkerSchedulerHist extends FwkApplication {

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
        static _scheduler_names = [
          'Group',
          'Slow',
          'Fast',
          'Med',
          'Snail'
        ];
        static _histogram_names = [
            'timeOfTransmittingTasks',
            'timeOfRunningTasks',
            'queuedTasks',
            'runningTasks',
            'transmittingTasks',
            'recentlyCompletedTasks'
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
  <th class="sticky">Scheduler</th>
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
<div class="row" id="fwk-qserv-scheduler-hist-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="scheduler-name">Scheduler:</label>
        <select id="scheduler-name" class="form-control form-control-selector">
          <option value="">&lt;any&gt;</option>`;
            for (let i in QservWorkerSchedulerHist._scheduler_names) {
                const name = QservWorkerSchedulerHist._scheduler_names[i];
                html += `
          <option value="Sched${name}">${name}</option>`;
            }
        html += `
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="histogram-name">Histogram:</label>
        <select id="histogram-name" class="form-control form-control-selector">`;
            for (let i in QservWorkerSchedulerHist._histogram_names) {
                const name = QservWorkerSchedulerHist._histogram_names[i];
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
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-scheduler-hist">
      <thead class="thead-light">
        ${QservWorkerSchedulerHist._table_head()}
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
              this._set_scheduler_name("");
              this._set_histogram_name(QservWorkerSchedulerHist._histogram_names[0]);
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
        _scheduler_name() { return this._form_control('select', 'scheduler-name').val(); }
        _set_scheduler_name(val) { this._form_control('select', 'scheduler-name').val(val); }

        /**
         * Table for displaying histograms that are being produced at workers.
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-scheduler-hist');
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
            const scheduler_name = this._scheduler_name();
            const histogram_name = this._histogram_name();
            let thead_html = QservWorkerSchedulerHist._table_head();
            let tbody_html = '';
            for (let worker in data) {
                if (!data[worker].success || _.isUndefined(data[worker].info.processor) ||
                                             _.isUndefined(data[worker].info.processor.queries) ||
                                             _.isUndefined(data[worker].info.processor.queries.blend_scheduler) ||
                                             _.isUndefined(data[worker].info.processor.queries.blend_scheduler.schedulers)) {
                    continue; 
                }
                let schedulers = data[worker].info.processor.queries.blend_scheduler.schedulers;
                if (_.isEmpty(schedulers)) continue;
                let rowspan = 1;
                let html   = '';
                for (let i in schedulers) {
                    let scheduler = schedulers[i];
                    if (!_.has(scheduler, "histograms")) continue;
                    let histograms = scheduler.histograms;
                    if (!_.has(histograms, histogram_name)) continue;
                    if (!_.isEmpty(scheduler_name) && (scheduler_name !== scheduler.name)) continue;
                    let histogram = histograms[histogram_name];
                    if (_.isEmpty(html)) {
                        thead_html = QservWorkerSchedulerHist._table_head(histogram);
                    }
                    html += `
<tr>
  <td><pre>${scheduler.name.substr(5)}</pre></td>
  <th style="text-align:right;"><pre>${histogram.total ? histogram.total.toFixed(3) : ''}</pre></th>
  <th style="text-align:right;"><pre>${histogram.totalCount ? histogram.totalCount : ''}</pre></th>
  <th style="text-align:right;"><pre>${histogram.avg ? histogram.avg.toFixed(3) : ''}</pre></th>`;
                    for (let i in histogram.buckets) {
                        let bucket = histogram.buckets[i];
                        html += `
  <th style="text-align:right;"><pre>${bucket.count ? bucket.count : ''}</pre></th>`;
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
            this._table().children('tbody').html(tbody_html);
        }
    }
    return QservWorkerSchedulerHist;
});
