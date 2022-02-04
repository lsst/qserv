define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/StatusWorkers.css');

    class StatusWorkers extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */ 
        static update_ival_sec() { return 10; }

        constructor(name) {
            super(name);
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }

        /**
         * Override event handler defined in the base class
         *
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > StatusWorkers.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
                }
            }
        }

        /**
         * The first time initialization of the page's layout
         */
        _init() {
            if (this._initialized === undefined) {
                this._initialized = false;
            }
            if (this._initialized) return;
            this._initialized = true;

            let html = `
<div class="row">
  <div class="col-md-4">
    <p>This dynamically updated table shows the status of <span style="font-weight:bold;">Worker</span>
      services in each category. A <span style="font-weight:bold;">Qserv</span> worker
      is supposed to be <span style="font-weight:bold;">OFF-LINE</span> if no response is
      received from the worker during the most recent <span style="font-weight:bold;">Health Monitoring probe</span>.
      In that case a non-zero value (the number of seconds) would be show in column
      <span style="font-weight:bold;">Last Response</span>.
      The state of the <span style="font-weight:bold;">Replication System</span>\s workers
      is a bit more complex. Workers in this category can be in one of the following
      states: <span style="font-weight:bold;">ENABLED</span>,
      <span style="font-weight:bold;">READ-ONLY</span>, or <span style="font-weight:bold;">DISABLED</span>.
      Note that the <span style="font-weight:bold;">Last Response</span> tracking for this type of
      workers is done in the first two categories only.
      Regardless of a status (or a response delay) of a worker, a number shown in the
      <span style="font-weight:bold;">#replicas</span> column will indicate either the actual number
      of replicas on the corresponding node, or the latest recorded number obtained from the last recorded scan
      of the worker node made by the <span style="font-weight:bold;">Replication System</span> before
      the worker service became non-responsive.
    </p>
  </div>
  <div class="col-md-8">
    <table class="table table-sm table-hover table-bordered" id="fwk-status-workers">
      <caption class="updating">
        Loading...
      </caption>
      <thead class="thead-light">
        <tr>
          <th rowspan="2" style="vertical-align:middle;">Worker</th>
          <th rowspan="2" style="vertical-align:middle; text-align:right;">#replicas</th>
          <th colspan="2" style="text-align:right">Qserv</th>
          <th colspan="2" style="text-align:right">Replication Sys.</th>
        </tr>
        <tr>
          <th style="text-align:right">Status</th>
          <th style="text-align:right">Last Response [s]</th>
          <th style="text-align:right">Status</th>
          <th style="text-align:right">Last Response [s]</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            this.fwk_app_container.html(html);
        }
        
        /**
         * 
         * @returns JQuery table object
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-status-workers');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web servie then render it to the application's
         * page.
         */
        _load() {
            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/worker",
                {},
                (data) => {
                    let html = '';
                    for (let i in data.workers) {
                        let workerInfo = data.workers[i];

                        let qservCssClass       = '';
                        let replicationCssClass = '';

                        if (workerInfo.qserv.probe_delay_s != 0) {
                            qservCssClass       = 'class="table-warning"';
                        }
                        if (workerInfo.replication.probe_delay_s != 0) {
                            replicationCssClass = 'class="table-warning"';
                        }
                        if ((workerInfo.qserv.probe_delay_s != 0) && (workerInfo.replication.probe_delay_s != 0)) {
                            qservCssClass       = 'class="table-danger"';
                            replicationCssClass = 'class="table-danger"';
                        }
                        let qservStatus = 'ON-LINE';
                        if (workerInfo.qserv.probe_delay_s != 0) {
                            qservStatus = 'OFF-LINE';
                        }
                        let replicationStatus = 'ENABLED';
                        if (workerInfo.replication.isEnabled) {
                            if (workerInfo.replication.isReadOnly) replicationStatus += 'READ-ONLY';
                        } else {
                            replicationStatus = 'DISABLED';
                            if (replicationCssClass === '') {
                                replicationCssClass = 'class="table-secondary"';
                            }
                        }
                        html += `
<tr>
  <td>`                                                           + workerInfo.worker                    + `</td>
  <td style="text-align:right"><pre>`                             + workerInfo.replication.num_replicas  + `</pre></td>
  <td style="text-align:right" ` + qservCssClass       + `>`      + qservStatus                          + `</td>
  <td style="text-align:right" ` + qservCssClass       + `><pre>` + workerInfo.qserv.probe_delay_s       + `</pre></td>
  <td style="text-align:right" ` + replicationCssClass + `>`      + replicationStatus                    + `</td>
  <td style="text-align:right" ` + replicationCssClass + `><pre>` + workerInfo.replication.probe_delay_s + `</pre></td>
</tr>`;
                    }
                    this._table().children('tbody').html(html);
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
    }
    return StatusWorkers;
});
