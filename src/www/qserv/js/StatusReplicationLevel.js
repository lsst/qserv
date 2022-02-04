define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/StatusReplicationLevel.css');

    class StatusReplicationLevel extends FwkApplication {
        
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
                if (now_sec - this._prev_update_sec > StatusReplicationLevel.update_ival_sec()) {
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
    <p>This dynamically updated table shows the <span style="font-weight:bold;">Act</span>ual replication
      levels for chunks across all known <span style="font-weight:bold;">Database</span>s.
      These levels may also be below or above the <span style="font-weight:bold;">Req</span>uired
      level which is set for each database <span style="font-weight:bold;">Family</span> in
      a configuration of the system. The levels may change for some chunks depending on
      a number of worker nodes which are <span style="font-weight:bold;">On-line</span> or
      <span style="font-weight:bold;">Inactive</span> (not responding) at a time when this
      table gets updated.
    </p>
    <p>Numbers under the
      <span style="font-weight:bold;">&plus;&nbsp;Inactive</span> columns
      represent a speculative scenario of <span style="font-style:italic;">what the
      replication level would be if</span> those nodes would also be
      <span style="font-weight:bold;">On-line</span> based on the last successful
      replica disposition scan of those nodes.
    </p>
    <p><span style="font-weight:bold;">HINT:</span> there seems to be a significant
      redundancy in the <span style="font-weight:bold;">Act</span>ual number of
      replicas well above the minimally <span style="font-weight:bold;">Req</span>uired
      level. Consider running the replica <span style="font-weight:bold;">Purge</span>
      tool.
    </p>
    <p><span style="font-weight:bold;">TODO:</span></p>
    <ul>
      <li>add a hyperlink to the Configuration section within this application</li>
      <li>add a hyperlink to the Workers tab to show a status of he workers</li>
      <li>add a hyperlink the replica <span style="font-weight:bold;">Purge</span> tool</li>
    </ul>
  </div>
  <div class="col-md-8">
    <table class="table table-sm table-hover table-bordered" id="fwk-status-level">
      <caption class="updating">
        Loading...
      </caption>
      <thead class="thead-light">
        <tr>
          <th rowspan="3" style="vertical-align:middle">Family</th>
          <th rowspan="3" style="vertical-align:middle">Req.</th>
          <th rowspan="3" style="vertical-align:middle">Database</th>
          <th rowspan="3" style="vertical-align:middle; text-align:right; border-right-color:#A9A9A9">Act.</th>
          <th colspan="4" style="text-align:right; border-right-color:#A9A9A9">Qserv</th>
          <th colspan="4" style="text-align:right">Replication Sys.</th>
        </tr>
          <th colspan="2" style="text-align:right">On-line</th>
          <th colspan="2" style="text-align:right; border-right-color:#A9A9A9">&plus;&nbsp;Inactive</th>
          <th colspan="2" style="text-align:right">On-line</th>
          <th colspan="2" style="text-align:right">&plus;&nbsp;Inactive</th>
        </tr>
        <tr>
          <th style="text-align:right">#chunks</th>
          <th style="text-align:right">%</th>
          <th style="text-align:right">#chunks</th>
          <th style="text-align:right; border-right-color:#A9A9A9">%</th>
          <th style="text-align:right">#chunks</th>
          <th style="text-align:right">%</th>
          <th style="text-align:right">#chunks</th>
          <th style="text-align:right">%</th>
        </tr>
      </thead>
      <tbody>
      </tbody>
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
                this._table_obj = this.fwk_app_container.find('table#fwk-status-level');
            }
            return this._table_obj;
        }

        static chunkNum2str(num) {
            return 0 == num ? '&nbsp;' : '' + num;
        }
        static percent2str(percent) {
            return 0 == percent ? '&nbsp;' : '' + percent.toFixed(2);
        }

        /**
         * Load data from a web servie then render it to the application's
         * page.
         */
        _load() {

            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');

            Fwk.web_service_GET(
                "/replication/level",
                {},
                (data) => {
                    let html = "";
                    for (let family in data.families) {
                        let familyInfo = data.families[family];
                        let familyRowSpan = 1;
                        let familyHtml = '';
                        for (let database in familyInfo.databases) {
                            let databaseInfo = familyInfo.databases[database];
                            let databaseRowSpan = 1;
                            familyRowSpan += databaseRowSpan;

                            // Rows for levels are going to be prepended to show then in the
                            // reverse (descending) order.
                            let databaseHtml = '';

                            for (let level in databaseInfo.levels) {
                                let levelInfo = databaseInfo.levels[level];

                                // Skip empty and insignificant levels
                                if (level < familyInfo.level) {
                                    let totalChunks =
                                        levelInfo.qserv.online.num_chunks +
                                        levelInfo.qserv.all.num_chunks +
                                        levelInfo.replication.online.num_chunks +
                                        levelInfo.replication.all.num_chunks;
                                    if (totalChunks == 0) continue;
                                }

                                // Otherwise count this row
                                databaseRowSpan++;
                                familyRowSpan++;

                                // Apply optional color schemes to rows depending on a value
                                // of the replication level relative to the required one.
                                let cssClass = '';
                                if      (level == 0)                cssClass = 'class="table-danger"';
                                else if (level == familyInfo.level) cssClass = 'class="table-success"';
                                else if (level <  familyInfo.level) cssClass = 'class="table-warning"';

                                databaseHtml = `
<tr ${cssClass}>
  <th style="text-align:center; border-right-color:#A9A9A9" scope="row"><pre>${level}</pre></th>
  <td style="text-align:right"><pre>${StatusReplicationLevel.chunkNum2str(levelInfo.qserv.online.num_chunks)}</pre></td>
  <td style="text-align:right"><pre>${StatusReplicationLevel.percent2str( levelInfo.qserv.online.percent)}</pre></td>
  <td style="text-align:right"><pre>${StatusReplicationLevel.chunkNum2str(levelInfo.qserv.all.num_chunks)}</pre></td>
  <td style="text-align:right; border-right-color:#A9A9A9"><pre>${StatusReplicationLevel.percent2str( levelInfo.qserv.all.percent)}</pre></td>
  <td style="text-align:right"><pre>${StatusReplicationLevel.chunkNum2str(levelInfo.replication.online.num_chunks)}</pre></td>
  <td style="text-align:right"><pre>${StatusReplicationLevel.percent2str( levelInfo.replication.online.percent)}</pre></td>
  <td style="text-align:right"><pre>${StatusReplicationLevel.chunkNum2str(levelInfo.replication.all.num_chunks)}</pre></td>
  <td style="text-align:right"><pre>${StatusReplicationLevel.percent2str( levelInfo.replication.all.percent)}</pre></td>
</tr>`+databaseHtml;
                            }
                            familyHtml += `
<tr>
  <td rowspan="${databaseRowSpan}" style="vertical-align:middle">${database}</td>
</tr>`+databaseHtml;
                        }
                        html += `
<tr>
  <td rowspan="`+familyRowSpan+`" style="vertical-align:middle">`+family+`</td>
  <th rowspan="`+familyRowSpan+`" style="vertical-align:middle; text-align:center;" scope="row"><pre>`+familyInfo.level+`</pre></th>
</tr>`;
                        html += familyHtml;
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
    return StatusReplicationLevel;
});
