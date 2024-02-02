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

    CSSLoader.load('qserv/css/QservCzarStatistics.css');

    class QservCzarStatistics extends FwkApplication {

      static czar_name = "proxy";   /// The name of Czar.

        constructor(name) {
            super(name);
            // The previous snapshot of the stats. It's used for  reporting "deltas"
            // in the relevant counters.
            this._prev = undefined;
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
        static _counters = [
            'queryRespConcurrentSetupCount',
            'queryRespConcurrentWaitCount',
            'queryRespConcurrentProcessingCount',
            'numQueries',
            'numJobs',
            'numResultFiles',
            'numResultMerges'
        ];
        static _totals = [
            'totalQueries',
            'totalJobs',
            'totalResultFiles',
            'totalResultMerges',
            'totalBytesRecv',
            'totalRowsRecv'
        ];
        static _totals_data_rate = new Set(['totalBytesRecv']);
        static _qdisppool_columns = ['priority', 'running', 'size'];

        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="row" id="fwk-qserv-czar-stats-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 2)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-form">&nbsp;</label>
        <button id="reset-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
  <table class="table table-sm fwk-qserv-czar-stats" id="fwk-qserv-czar-stats-status">
    <caption class="updating">Loading...</caption>
  </table>
  </div>
</div>
<div class="row">
  <div class="col col-md-5">
    <h4>Integrated Totals</h4>
    <table class="table table-sm table-hover fwk-qserv-czar-stats" id="fwk-qserv-czar-stats-totals">
      <thead class="thead-light">
        <tr>
          <th>&nbsp;</th>
          <th style="text-align:right;">&Sum;</th>
          <th style="text-align:right; width:6em;">s<sup>-1</sup></th>
          <th style="text-align:left; width:3em;">&nbsp;</th>
          <th style="text-align:right; width:8em;">&Delta;</th>
          <th style="text-align:right; width:6em;">s<sup>-1</sup></th>
          <th style="text-align:left; width:3em;">&nbsp;</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td style="text-align:left" scope="row"><pre>runTime</pre></td>
          <th style="text-align:right"><pre id="runTime">Loading...</pre></th>
          <td>&nbsp;</td>
          <td>&nbsp;</td>
          <td>&nbsp;</td>
          <td>&nbsp;</td>
          <td>&nbsp;</td>
        </tr>` + _.reduce(QservCzarStatistics._totals, function(html, counter) { return html + `
        <tr>
          <td style="text-align:left" scope="row"><pre>${counter}</pre></td>
          <th style="text-align:right"><pre id="${counter}">Loading...</pre></th>
          <td style="text-align:right"><pre id="${counter}_perf_sum" class="perf"></pre></td>
          <td style="text-align:left"><pre id="${counter}_unit_sum" class="perf"></pre></td>
          <td style="text-align:right"><pre id="${counter}_delta"></pre></td>
          <td style="text-align:right"><pre id="${counter}_perf" class="perf"></pre></td>
          <td style="text-align:left"><pre id="${counter}_unit" class="perf"></pre></td>
        </tr>`; }, '') + `
      </tbody>
    </table>
  </div>
  <div class="col col-md-4">
    <h4>Running Counters</h4>
    <table class="table table-sm table-hover fwk-qserv-czar-stats" id="fwk-qserv-czar-stats-counters">
      <thead class="thead-light">
        <tr>
          <th>&nbsp;</th>
          <th style="text-align:right; width:3em;">current</th>
          <th style="text-align:right; width:6em;">&Delta;</th>
          <th style="text-align:right; width:6em;">s<sup>-1</sup></th>
        </tr>
      </thead>
      <tbody>` + _.reduce(QservCzarStatistics._counters, function(html, counter) { return html + `
        <tr>
          <td style="text-align:left" scope="row"><pre>${counter}</pre></td>
          <th style="text-align:right"><pre id="${counter}">Loading...</pre></th>
          <td style="text-align:right"><pre id="${counter}_delta" class="delta"></pre></td>
          <td style="text-align:right"><pre id="${counter}_perf"  class="perf"></pre></td>
        </tr>`; }, '') + `
      </tbody>
    </table>
  </div>
  <div class="col">
    <h4>QdispPool</h3>
    <table class="table table-sm table-hover fwk-qserv-czar-stats" id="fwk-qserv-czar-stats-qdisppool">
      <thead class="thead-light">
        <tr>` + _.reduce(QservCzarStatistics._qdisppool_columns, function(html, column) { return html + `
          <th class="sticky" style="text-align:right;">${column}</th>`; }, '') + `
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <h4>Timing Histograms</h4>
    <table class="table table-sm table-hover fwk-qserv-czar-stats" id="fwk-qserv-czar-stats-timing"></table>
  </div>
</div>
<div class="row">
  <div class="col">
    <h4>Data Rates Histograms</h4>
    <table class="table table-sm table-hover fwk-qserv-czar-stats" id="fwk-qserv-czar-stats-data"></table>
  </div>
</div>
`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-selector").change(() => {
                this._load();
            });
            cont.find("button#reset-form").click(() => {
                this._set_update_interval_sec(2);
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
        _table(name) {
            if (_.isUndefined(this._table_obj)) this._table_obj = {};
            if (!_.has(this._table_obj, name)) {
                this._table_obj[name] = this.fwk_app_container.find('table#fwk-qserv-czar-stats-' + name);
            }
            return this._table_obj[name];
        }
        _status() {
            if (_.isUndefined(this._status_obj)) {
                this._status_obj = this._table('status').children('caption');
            }
            return this._status_obj;
        }
        _set_counter(table, counter, val) {
            this._set(table, counter, Number(val).toLocaleString());
        }
        _set_counter_delta(table, counter, val) {
            this._set(table, counter + "_delta", val == 0 ? '' : Number(val).toLocaleString());
        }
        _set_counter_perf(table, counter, valAndUnit, suffix='') {
            console.log(table, counter, valAndUnit);
            const val_unit = valAndUnit.split(' ');
            const val = val_unit[0];
            const unit = val_unit.length > 1 ? val_unit[1] : '';
            if (val == 0) {
                this._set(table, counter + "_perf" + suffix, '');
                this._set(table, counter + "_unit" + suffix, '');
            } else {
                this._set(table, counter + "_perf" + suffix, Number(val).toLocaleString());
                this._set(table, counter + "_unit" + suffix, unit);
            }
        }
        _set(table, counter_id, val) {
            if (_.isUndefined(this._counters_obj)) this._counters_obj = {};
            if (_.isUndefined(this._counters_obj[table])) this._counters_obj[table] = {};
            if (!_.has(this._counters_obj[table], counter_id)) {
                this._counters_obj[table][counter_id] = this._table(table).children('tbody').find('#' + counter_id);
            }
            this._counters_obj[table][counter_id].text(val);
        }
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._status().addClass('updating');
            Fwk.web_service_GET(
                "/replication/qserv/master/status/" + QservCzarStatistics.czar_name,
                {   timeout_sec: 2,
                    version: Common.RestAPIVersion},
                (data) => {
                    if (data.success) {
                        this._display(data.status);
                        Fwk.setLastUpdate(this._status());
                    } else {
                        console.log('request failed', this.fwk_app_name, data.error);
                        this._status().html('<span style="color:maroon">' + data.error + '</span>');
                    }
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _display(data) {
            let tbody = this._table('qdisppool').children('tbody');
            if (_.isEmpty(data) || _.isEmpty(data.qdisp_stats) || _.isEmpty(data.qdisp_stats.QdispPool)) {
                tbody.html('');
                return;
            }
            let that = this;
            const runTimeSec = Math.round((data.qdisp_stats.snapshotTimeMs - data.qdisp_stats.startTimeMs) / 1000);
            this._set('totals', 'runTime', QservCzarStatistics._elapsed(runTimeSec));
            _.each(QservCzarStatistics._totals, function(counter) {
                that._set_counter('totals', counter, data.qdisp_stats[counter]);
                if (runTimeSec > 0) {
                    const perf = data.qdisp_stats[counter] / runTimeSec;
                    if (QservCzarStatistics._totals_data_rate.has(counter)) {
                        that._set_counter_perf('totals', counter, Common.format_data_rate(perf), '_sum');
                    } else {
                        that._set_counter_perf('totals', counter, perf.toFixed(0), '_sum');
                    }
                }
                if (!_.isUndefined(that._prev)) {
                    const deltaVal = data.qdisp_stats[counter] - that._prev.qdisp_stats[counter];
                    that._set_counter_delta('totals', counter, deltaVal);
                    const deltaT = (data.qdisp_stats.snapshotTimeMs - that._prev.qdisp_stats.snapshotTimeMs) / 1000;
                    if (deltaT > 0) {
                        const perf = deltaVal / deltaT;
                        if (QservCzarStatistics._totals_data_rate.has(counter)) {
                            that._set_counter_perf('totals', counter, Common.format_data_rate(perf));
                        } else {
                            that._set_counter_perf('totals', counter, perf.toFixed(0));
                        }
                    }
                }
            });
            _.each(QservCzarStatistics._counters, function(counter) {
                that._set_counter('counters', counter, data.qdisp_stats[counter]);
                if (!_.isUndefined(that._prev)) {
                    const deltaVal = data.qdisp_stats[counter] - that._prev.qdisp_stats[counter];
                    that._set_counter_delta('counters', counter, deltaVal);
                    const deltaT = (data.qdisp_stats.snapshotTimeMs - that._prev.qdisp_stats.snapshotTimeMs) / 1000;
                    if (deltaT > 0) {
                        that._set_counter_perf('counters', counter, (deltaVal / deltaT).toFixed(0));
                    }
                }
            });
            let html = '';
            _.each(data.qdisp_stats.QdispPool, function (row) {
                html += `
<tr>` + _.reduce(QservCzarStatistics._qdisppool_columns, function (html, column) { return html + `
  <td style="text-align:right;"><pre>${row[column]}</pre></td>`; }, '') + `
</tr>`;
            });
            this._table('qdisppool').children('tbody').html(html);

            // Locate and display histograms nested in the top-level objects
            this._table('timing').html(this._htmlgen_histograms(
                _.reduce(data.qdisp_stats, function (histograms, e) {
                    if (_.isObject(e) && _.has(e, 'HistogramId')) histograms.push(e);
                    return histograms;
                }, [])
            ));
            this._table('data').html(this._htmlgen_histograms(
                _.reduce(data.transmit_stats, function (histograms, e) {
                    if (_.isObject(e) && _.has(e, 'HistogramId')) histograms.push(e);
                    return histograms;
                }, []),
                true
            ));
            this._prev = data;
        }
        _htmlgen_histograms(histograms, data_rate = false) {
            return _.reduce(histograms, function (html, histogram) {
                if (html  == '') {
                    let idx = 0;
                    html = `
<thead class="thead-light">
  <tr>
    <th class="sticky" style="text-align:left;">id</th>`;
                    if (data_rate) {
                      html += `
    <th class="sticky" style="text-align:right;">avg</th>`;
                    } else {
                        html += `
    <th class="sticky" style="text-align:right;">total</th>
    <th class="sticky" style="text-align:right;">totalCount</th>
    <th class="sticky" style="text-align:right;">avg</th>`;
                    }
                    html += _.reduce(histogram.buckets, function (html, bucket) { return html + `
    <th class="sticky" style="text-align:right;">${(idx++) == 0 ? "&le;&nbsp;" : ""}${QservCzarStatistics._format_bucket_limit(bucket.maxVal, data_rate)}</th>`; }, '') + `
  </tr>
</thead>
<tbody>`;
                }
                html += `
  <tr>
    <th style="text-align:left;">${histogram.HistogramId}</th>`;
                if (data_rate) {
                    html += `
    <td style="text-align:right;"><pre>${Math.round(histogram.avg).toLocaleString()}</pre></td>`;
                } else {
                    html += `
    <td style="text-align:right;"><pre>${histogram.total.toFixed(3)}</pre></td>
    <td style="text-align:right;"><pre>${histogram.totalCount}</pre></td>
    <td style="text-align:right;"><pre>${histogram.avg.toFixed(3)}</pre></td>`;
                }
                html += _.reduce(histogram.buckets, function (html, bucket) { return html + `
    <td style="text-align:right;"><pre>${bucket.count}</pre></td>`; }, '') + `
  </tr>`;
                return html;
            }, '') + `
</tbody>`;
        }
        static _format_bucket_limit(v, data_rate=false) {
            if (isNaN(v)) return v;
            if (data_rate) {
                if (v < Common.KB) return v + " B/s";
                else if (v < Common.MB) return (v / Common.KB).toFixed(0) + " KB/s";
                else if (v < Common.GB) return (v / Common.MB).toFixed(0) + " MB/s";
                return (v / Common.GB).toFixed(0) + " GB/s";
            }
            return v.toLocaleString();
        }

        /**
         * @param {Number} seconds
         * @returns {String} the amount of time elapsed by a query, formatted as: 'hh:mm:ss'
         */
        static _elapsed(totalSeconds) {
          let hours   = Math.floor(totalSeconds / 3600);
          let minutes = Math.floor((totalSeconds - 3600 * hours) / 60);
          let seconds = (totalSeconds - 3600 * hours - 60 * minutes) % 60;
          let displayHours   = hours !== 0;
          let displayMinutes = displayHours || minutes !== 0;
          let displaySeconds = true;
          return (displayHours   ? (hours   < 10 ? '0' : '') + hours   + ':' : '') +
                 (displayMinutes ? (minutes < 10 ? '0' : '') + minutes + ':' : '') +
                 (displaySeconds ? (seconds < 10 ? '0' : '') + seconds : '');
      }
    }
    return QservCzarStatistics;
});
