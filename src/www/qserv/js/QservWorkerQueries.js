define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/QservWorkerQueries.css');

    class QservWorkerQueries extends FwkApplication {


        /// @returns the default update interval for the page
        static update_ival_sec() { return 2; }

        constructor(name) {
            super(name);
        }

        /// @see FwkApplication.fwk_app_on_show
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /// @see FwkApplication.fwk_app_on_hide
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }

        /// @see FwkApplication.fwk_app_on_update
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > QservWorkerQueries.update_ival_sec()) {
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
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            this._prevTimestamp = 0;

            let html = `
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-queries">
      <thead class="thead-light">
        <tr>
          <th>worker</th>
          <th>scheduler</th>
          <th>#tasks</th>
          <th>qid</th>
          <th>query</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            this.fwk_app_container.html(html);
        }
        
        /// @returns JQuery table object displaying XRootD/SSI resources on each worker
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-queries');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web servie then render it to the application's page.
         */
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');

            Fwk.web_service_GET(
                "/replication/qserv/worker/status",
                {'timeout_sec': 2},
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
        
        static arrayOfPairs2str(a) {
            return _.reduce(a, function(s, pair){ return s + '<pre>' + pair[0] + ' (' + pair[1] + ')</pre> '; }, '');

        }

        _display(data) {
            let html = '';
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
                            let queryId  = scheduler.query_id_to_count[j][0];
                            let numTasks = scheduler.query_id_to_count[j][1];
                            htmlSchedulerQueries += `
<tr>
  <td><pre>${numTasks}</pre></td>
  <td><pre>${queryId}</pre></td>
  <td><pre>${queries[queryId].query}<pre></td>
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
            this._table().children('tbody').html(html);
        }

    }
    return QservWorkerQueries;
});
