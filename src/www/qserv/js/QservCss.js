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

    CSSLoader.load('qserv/css/QservCss.css');

    class QservCss extends FwkApplication {

        /**
         * @returns the default update interval for the page.
         */ 
        static update_ival_sec() { return 10; }

        constructor(name) {
            super(name);
        }

        /**
         * Override event handler defined in the base class.
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class.
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {}

        /**
         * Override event handler defined in the base class.
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > QservCss.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
                }
            }
        }

        /**
         * The first time initialization of the page's layout.
         */
        _init() {
            if (this._initialized === undefined) {
                this._initialized = false;
            }
            if (this._initialized) return;
            this._initialized = true;

            let html = `
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-borderless" id="fwk-qserv-css">
      <thead class="thead-light">
        <tr>
          <th class="sticky">FAMILY</th>
          <th class="sticky">DATABASE</th>
          <th class="sticky">TABLE</th>
          <th class="sticky">lockInMem</th>
          <th class="sticky">scanRating</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            this.fwk_app_container.html(html);
        }

       /**
         * Table for displaying CSS parameters.
         * @returns JQuery table object
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-css');
            }
            return this._table_obj;
        }
        _load() {
            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/qserv/css/shared-scan",
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._on_failed(data.error);
                        return;
                    }
                    this._display(data["css"]["shared_scan"]);
                    Fwk.setLastUpdate(this._table().children('caption'));
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                },
                (msg) => { this._on_failed('No Response'); }
            );
        }
        _on_failed(msg) {
            this._table().children('caption').html('<span style="color:maroon">' + msg + '</span>');
            this._table().children('caption').removeClass('updating');
            this._loading = false;
        }
        _display(families) {
            console.log('families', families);
            let html = '';
            for (let family in families) {
                let databases = families[family];
                console.log('databases',databases);
                let familyRowSpan = 1;
                let familyHtml = '';
                for (let database in databases) {
                    let tables = databases[database];
                    console.log('tables',tables);
                    let databaseRowSpan = 1;
                    familyRowSpan += databaseRowSpan;
                    let databaseHtml = '';
                    let tableIdx = 0;
                    for (let table in tables) {
                        let sharedScan = tables[table];
                        databaseRowSpan++;
                        familyRowSpan++;
                        const tableSchemaSupportCSS = `class="database_table" database="${database}" table="${table}"`;
                        const lockInMem = _.has(sharedScan, 'lockInMem') ? (sharedScan.lockInMem ? '<b>yes</b>' : 'no') : '&nbsp;';
                        const scanRating = _.has(sharedScan, 'scanRating') ? sharedScan.scanRating : '&nbsp;';
                        databaseHtml += `
<tr ` + (tableIdx++ === tables.length - 1 ? ' style="border-bottom: solid 1px #dee2e6"' : '') + `>
  <td scope="row"><pre ${tableSchemaSupportCSS}>${table}</pre></td>
  <td><pre>${lockInMem}</pre></td>
  <td><pre>${scanRating}</pre></td>
</tr>`;
                    }
                    familyHtml += `
<tr style="border-bottom: solid 1px #dee2e6">
  <td rowspan="${databaseRowSpan}" style="vertical-align:middle;">${database}</td>
</tr>` + databaseHtml;
                }
                html += `
<tr style="border-bottom: solid 1px #dee2e6">
  <th rowspan="${familyRowSpan}" style="vertical-align:middle" scope="row">${family}</th>
</tr>` + familyHtml;
            }
            this._table().children('tbody').html(html).find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
        }
    }
    return QservCss;
});
