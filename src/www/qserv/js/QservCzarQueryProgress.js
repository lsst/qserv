define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'qserv/Common',
    'underscore',
    'highcharts',
    'highcharts/modules/exporting',
    'highcharts/modules/accessibility'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         Common,
         _,
         Highcharts) {

    CSSLoader.load('qserv/css/QservCzarQueryProgress.css');

    class QservCzarQueryProgress extends FwkApplication {

        // Return the default number of the last seconds to track in the query history
        static last_seconds() { return 15 * 60; }

        constructor(name) {
            super(name);
            this._data = undefined;
            this._queries_chart = undefined;
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
        /// Set the identifier and begin loading the query info in the background.
        set_query_id(czar, query_id) {
            this._init();
            this._set_czar(czar);
            this._set_query_id(query_id);
            this._set_last_seconds(24 * 3600);
            this._load(czar);
        }
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            const lastMinutes = [1, 3, 5, 15, 30, 45];
            const lastHours = [1, 2, 4, 8, 12, 16, 20, 24];
            let html = `
<div class="row" id="fwk-qserv-czar-query-prog-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        <label for="czar">Czar:</label>
        <select id="czar" class="form-control form-control-selector">
        </select>
      </div>
      <div class="form-group col-md">
        <label for="query-id">Query Id:</label>
        <input type="number" id="query-id" class="form-control" value="">
      </div>
      <div class="form-group col-md">
        <label for="last-seconds">Track last:</label>
        <select id="last-seconds" class="form-control form-control-selector">` + _.reduce(lastMinutes, function (html, m) { return html + `
          <option value="${m * 60}">${m} min</option>`; }, '') + _.reduce(lastHours, function (html, hr) { return html + `
          <option value="${hr * 3600}">${hr} hr</option>`; }, '') + `
        </select>
      </div>
      <div class="form-group col-md"
           title="Zero values are not plotted in the logarithmic scale"
           data-toggle="tooltip"
           data-placement="top">
        <label for="vertical-scale">Vertical scale:</label>
        <select id="vertical-scale" class="form-control form-control-viewer">
          <option selected value="logarithmic">logarithmic</option>
          <option value="linear">linear</option>
        </select>
      </div>
      <div class="form-group col-md"
           title="Enabling auto-zoom would expand the plot all the way to the right"
           data-toggle="tooltip"
           data-placement="top">
        <label for="horizontal-scale">Horizontal scale:</label>
        <select id="horizontal-scale" class="form-control form-control-viewer">
          <option value="">&nbsp;</option>
          <option selected value="auto-zoom-in">auto-zoom-in</option>
        </select>
      </div>
      <div class="form-group col-md">
        ${Common.html_update_ival('update-interval', 10)}
      </div>
      <div class="form-group col-md">
        <label for="reset-form">&nbsp;</label>
        <button id="reset-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
  <table class="table table-sm" id="fwk-qserv-czar-query-prog-status">
    <caption class="updating">Loading...</caption>
  </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <div id="fwk-qserv-czar-query-prog-status-queries"></div>
  </div>
</div>
`;
            let cont = this.fwk_app_container.html(html);
            cont.find('[data-toggle="tooltip"]').tooltip();
            this._set_last_seconds(QservCzarQueryProgress.last_seconds());
            cont.find(".form-control-selector").change(() => {
                this._load();
            });
            cont.find(".form-control#query-id").change(() => {
                if (this._query_id()) this._set_last_seconds(24 * 3600);
                this._load();
            });
            cont.find(".form-control-viewer").change(() => {
                if (_.isUndefined(this._data)) this._load();
                else this._display(this._data.queries);
            });
            cont.find("button#reset-form").click(() => {
                this._set_update_interval_sec(10);
                this._set_query_id('');
                this._set_last_seconds(QservCzarQueryProgress.last_seconds());
                this._set_vertical_scale('logarithmic');
                this._set_horizontal_scale('auto-zoom-in');
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
        _czar() { return this._form_control('select', 'czar').val(); }
        _set_czar(val) { this._form_control('select', 'czar').val(val); }
        _set_czars(czars) {
            const prev_czar = this._czar();
            let html = '';
            for (let i in czars) {
                const czar = czars[i];
                const selected = (_.isEmpty(prev_czar) && (i === 0)) ||
                                 (!_.isEmpty(prev_czar) && (prev_czar === czar.name));
                html += `
 <option value="${czar.name}" ${selected ? "selected" : ""}>${czar.name}[${czar.id}]</option>`;
            }
            this._form_control('select', 'czar').html(html);
        }
        _query_id() { return this._form_control('input', 'query-id').val(); }
        _set_query_id(val) {
            this._form_control('input', 'query-id').val(val);
        }
        _vertical_scale() { return this._form_control('select', 'vertical-scale').val(); }
        _set_vertical_scale(val) { this._form_control('select', 'vertical-scale').val(val); }
        _horizontal_scale() { return this._form_control('select', 'horizontal-scale').val(); }
        _set_horizontal_scale(val) { this._form_control('select', 'horizontal-scale').val(val); }
        _last_seconds() { return this._form_control('select', 'last-seconds').val(); }
        _set_last_seconds(val) { this._form_control('select', 'last-seconds').val(val); }
        _table(name) {
            if (_.isUndefined(this._table_obj)) this._table_obj = {};
            if (!_.has(this._table_obj, name)) {
                this._table_obj[name] = this.fwk_app_container.find('table#fwk-qserv-czar-query-prog-' + name);
            }
            return this._table_obj[name];
        }
        _status() {
            if (_.isUndefined(this._status_obj)) {
                this._status_obj = this._table('status').children('caption');
            }
            return this._status_obj;
        }
        _queries() {
            if (_.isUndefined(this._queries_obj)) {
                this._queries_obj = this.fwk_app_container.find('canvas#queries');
            }
            return this._queries_obj;
        }
        _load(czar = undefined) {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/config",
                {   timeout_sec: 2,
                    version: Common.RestAPIVersion},
                (data) => {
                    if (data.success) {
                        this._set_czars(data.config.czars);
                        if (!_.isUndefined(czar)) this._set_czar(czar);
                        this._load_query();
                    } else {
                        console.log('request failed', this.fwk_app_name, data.error);
                        this._status().html('<span style="color:maroon">' + data.error + '</span>');
                    }
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _load_query() {
            Fwk.web_service_GET(
                "replication/qserv/master/queries/active/progress/" + this._czar(),
                {   version: Common.RestAPIVersion,
                    timeout_sec: 2,
                    query_id: this._query_id() ? this._query_id() : 0,
                    last_seconds: this._last_seconds()
                },
                (data) => {
                    if (data.success) {
                        this._data = data;
                        this._display(data.queries);
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
        _display(queries) {
            const query_ids = _.keys(queries);
            query_ids.sort();
            query_ids.reverse();
            // Add a small delta to the points to allow seeing zeroes on the log scale,
            // in case the one was requested.
            const valueDeltaForLogScale = this._vertical_scale() === 'linear' ? 0 : 0.1;
            let series = [];
            for (let qid in queries) {
                let points = [];
                let query_data = queries[qid];
                for (let i in query_data) {
                    const point = query_data[i];
                    // Convert from milliseconds
                    const timestampSec = point[0] / 1000;
                    let x = new Date(0);
                    x.setSeconds(timestampSec);
                    points.push([x.getTime(), point[1] + valueDeltaForLogScale]);
                }
                series.push({
                    name: qid,
                    data: points,
                    animation: {
                        enabled: false
                    },
                    showInLegend: false,
                    // color: '#F00',
                });
            }
            if (!_.isUndefined(this._queries_chart)) {
                this._queries_chart.destroy();
            }
            this._queries_chart = Highcharts.chart('fwk-qserv-czar-query-prog-status-queries', {
                chart: {
                    type: 'line'
                },
                title: {
                    text: '# Unfinished Jobs'
                },
                subtitle: {
                    text: '< 24 hours'
                },
                xAxis: {
                    type: 'datetime',
                    title: {
                        text: 'Time [UTC]'
                    },
                    // If auto-zoom is not enabled the plot will go all the way through
                    // the (viewer's) current time on the right.
                    max: this._horizontal_scale() === 'auto-zoom-in' ? undefined : new Date().setSeconds(0)
                },
                yAxis: {
                    type: this._vertical_scale(),
                    title: {
                        text: '# jobs'
                    }
                },
                tooltip: {
                    headerFormat: '<b>{series.name}</b><br>',
                    pointFormat: '{point.x:%e. %b}: {point.y:.2f} jobs'
                },
                time: {
                    useUTC: true
                },
                plotOptions: {
                    series: {
                        marker: {
                            fillColor: '#dddddd',
                            lineWidth: 2,
                            lineColor: null
                        }
                    }
                },
                colors: ['#6CF', '#39F', '#06C', '#036', '#000'],
                series: series
            });
        }
    }
    return QservCzarQueryProgress;
});
