define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'qserv/Common',
    'underscore'],

    function (CSSLoader,
        Fwk,
        FwkApplication,
        Common,
        _) {

        CSSLoader.load('qserv/css/ReplicationControllerStats.css');

        class ReplicationControllerStats extends FwkApplication {
            constructor(name) {
                super(name);
                this._prev_update_sec = 0;  // for triggering page updates
                this._prev_stats = undefined;                
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
                        this._load();
                    }
                }
            }
            _init() {
                if (this._initialized === undefined) {
                    this._initialized = false;
                }
                if (this._initialized) return;
                this._initialized = true;
                let html = `
<div class="row" id="fwk-controller-stats-controls">
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
    <table class="table table-sm table-hover table-bordered" id="fwk-controller-stats">
      <thead class="thead-light">
        <tr>
          <th class="sticky left-aligned">Worker</th>
          <th class="sticky right-aligned">Requests</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Error counters</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Data</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&nbsp;</th>
        </tr>
        <tr>
          <th class="sticky left-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Submitted</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Queued</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">In-progress</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Resolves</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Connects</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Sends</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">Receives</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">TX</th>
          <th class="sticky right-aligned">&nbsp;</th>
          <th class="sticky right-aligned">RX</th>
          <th class="sticky right-aligned">&nbsp;</th>
        </tr>
        <tr>
          <th class="sticky left-aligned">&nbsp;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:2em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:2em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:2em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:1em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:1em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:1em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:1em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:5em;">&Delta;</th>
          <th class="sticky right-aligned">&Sum;</th>
          <th class="sticky right-aligned" style="min-width:5em;">&Delta;</th>
        </tr>
      </thead>
      <caption>Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
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
            _table() {
                if (_.isUndefined(this._table_obj)) {
                    this._table_obj = this.fwk_app_container.find('table#fwk-controller-stats');
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
                Fwk.web_service_GET(
                    "/replication/protocol/controller/stats",
                    {
                        timeout_sec: 2,
                        version: Common.RestAPIVersion
                    },
                    (data) => {
                        if (data.success) {
                            this._display(data.stats);
                            Fwk.setLastUpdate(this._status());
                        } else {
                            console.log('request failed', this.fwk_app_name, data.error);
                            this._status().html('<span style="color:maroon">' + data.error + '</span>');
                        }
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
            _display(stats) {
                if (_.isUndefined(this._prev_stats)) this._prev_stats = {};
                for (let worker in stats) {
                    if (_.isUndefined(this._prev_stats[worker])) {
                        this._prev_stats[worker] = {
                            requestCount: 0,
                            queuedRequests: 0,
                            activeRequests: 0,
                            errorCountResolves: 0,
                            errorCountConnects: 0,
                            errorCountSends: 0,
                            errorCountReceives: 0,
                            dataTx: 0,
                            dataRx: 0
                        };
                    }
                }

                let html = '';
                for (let worker in stats) {
                    let delta_stats = {
                        requestCount: stats[worker].requestCount - this._prev_stats[worker].requestCount,
                        queuedRequests: stats[worker].queuedRequests - this._prev_stats[worker].queuedRequests,
                        activeRequests: stats[worker].activeRequests - this._prev_stats[worker].activeRequests,
                        errorCountResolves: stats[worker].errorCountResolves - this._prev_stats[worker].errorCountResolves,
                        errorCountConnects: stats[worker].errorCountConnects - this._prev_stats[worker].errorCountConnects,
                        errorCountSends: stats[worker].errorCountSends - this._prev_stats[worker].errorCountSends,
                        errorCountReceives: stats[worker].errorCountReceives - this._prev_stats[worker].errorCountReceives,
                        dataTx: stats[worker].dataTx - this._prev_stats[worker].dataTx,
                        dataRx: stats[worker].dataRx - this._prev_stats[worker].dataRx
                    };
                    html += `
<tr>
  <th style="text-align:left;">${worker}</th>
  <th style="text-align:right;"><pre>${stats[worker].requestCount ? stats[worker].requestCount : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.requestCount ? delta_stats.requestCount : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].queuedRequests ? stats[worker].queuedRequests : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.queuedRequests ? delta_stats.queuedRequests : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].activeRequests ? stats[worker].activeRequests : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.activeRequests ? delta_stats.activeRequests : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].errorCountResolves ? stats[worker].errorCountResolves : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.errorCountResolves ? delta_stats.errorCountResolves : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].errorCountConnects ? stats[worker].errorCountConnects : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.errorCountConnects ? delta_stats.errorCountConnects : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].errorCountSends ? stats[worker].errorCountSends : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.errorCountSends ? delta_stats.errorCountSends : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].errorCountReceives ? stats[worker].errorCountReceives : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.errorCountReceives ? delta_stats.errorCountReceives : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].dataTx ? Common.format_data_rate(stats[worker].dataTx) : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.dataTx ? Common.format_data_rate(delta_stats.dataTx) : ''}</pre></td>
  <th style="text-align:right;"><pre>${stats[worker].dataRx ? Common.format_data_rate(stats[worker].dataRx) : ''}</pre></th>
  <td style="text-align:right;"><pre>${delta_stats.dataRx ? Common.format_data_rate(delta_stats.dataRx) : ''}</pre></td>
</tr>`;
                }
                this._prev_stats = stats;
                this._table().find('tbody').html(html);
            }
        }
        return ReplicationControllerStats;
    });
