define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/ReplicationConfigCatalogs.css');

    class ReplicationConfigCatalogs extends FwkApplication {

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
                if (now_sec - this._prev_update_sec > ReplicationConfigCatalogs.update_ival_sec()) {
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
    <table class="table table-sm table-hover table-borderless" id="fwk-controller-config-catalogs">
      <thead class="thead-light">
        <tr>
          <th class="sticky">Family</th>
          <th class="sticky">stripes</th>
          <th class="sticky">sub-stripes</th>
          <th class="sticky">repl level</th>
          <th class="sticky">Database</th>
          <th class="sticky">is pub-d</th>
          <th class="sticky">Table</th>
          <th class="sticky">is part-d</th>
          <th class="sticky">is dir</th>
          <th class="sticky">director table</th>
          <th class="sticky">director key</th>
          <th class="sticky">latitude key</th>
          <th class="sticky">longitude key</th>
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
         * Table for displaying Configuration parameters of the catalogs.
         * @returns JQuery table object
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-controller-config-catalogs');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web servie then render it to the application's page.
         */
        _load() {
            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/config",
                {},
                (data) => {
                    this._display(data);
                    Fwk.setLastUpdate(this._table().children('caption'));
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }

        /**
         * Display the configuration.
         */
        _display(data) {
            var config = data.config;

            // Organize family descriptors as a dictionary where the key would be
            // the name of a family. Extend each family descriptor with an array
            // storying the dependent database descriptors.
            // will get stored
            let families = {};
            for (let i in config.database_families) {
                let familyInfo = config.database_families[i];
                familyInfo['databases'] = [];
                families[familyInfo.name] = familyInfo;
            }
            for (let i in config.databases) {
                let databaseInfo = config.databases[i];
                families[databaseInfo.family_name]['databases'].push(databaseInfo);
            }

            let html = '';
            for (let i in families) {
                let family = families[i];
                let familyRowSpan = 1;
                let familyHtml = '';
                for (let j in family['databases']) {
                    let database = family['databases'][j];
                    let databaseRowSpan = 1;
                    familyRowSpan += databaseRowSpan;

                    let databaseHtml = '';
                    for (let k in database.tables) {
                        let table = database.tables[k];
                        databaseRowSpan++;
                        familyRowSpan++;
                        const tableSchemaSupportCSS = `class="database_table" database="${database.database}" table="${table.name}"`;
                        const isPartitionedStr = table.is_partitioned ? '<b>yes</b>' : 'no';
                        const isDirectorStr = table.is_partitioned && (table.director === "") ? '<b>yes</b>' : 'no';
                        const directorTable = table.is_partitioned ? table.director : '&nbsp;';
                        const directorTableSchemaSupportCSS = table.is_partitioned && (table.director === "") ? ""
                                : `class="database_table" database="${database.database}" table="${table.director}"`;
                        const directorKey = table.is_partitioned ? table.director_key : '&nbsp;';
                        const latitudeKey = table.is_partitioned ? table.latitude_key : '&nbsp;';
                        const longitudeKey = table.is_partitioned ? table.longitude_key : '&nbsp;';
                        databaseHtml += `
<tr ` + (k == database.tables.length - 1 ? ' style="border-bottom: solid 1px #dee2e6"' : '') + `>
  <td scope="row"><pre ${tableSchemaSupportCSS}>${table.name}</pre></td>
  <td><pre>${isPartitionedStr}</pre></td>
  <td><pre>${isDirectorStr}</pre></td>
  <td><pre ${directorTableSchemaSupportCSS}>${directorTable}</pre></td>
  <td><pre>${directorKey}</pre></td>
  <td><pre>${latitudeKey}</pre></td>
  <td><pre>${longitudeKey}</pre></td>
</tr>`;
                    }
                    familyHtml += `
<tr style="border-bottom: solid 1px #dee2e6">
  <td rowspan="${databaseRowSpan}" style="vertical-align:middle;">${database.database}</td>
  <td rowspan="${databaseRowSpan}" style="vertical-align:middle; border-right: solid 1px #dee2e6">${database.is_published ? '<b>yes</b>' : 'no'}</td>
</tr>` + databaseHtml;
                }
                html += `
<tr style="border-bottom: solid 1px #dee2e6">
  <th rowspan="${familyRowSpan}" style="vertical-align:middle" scope="row">${family.name}</th>
  <td rowspan="${familyRowSpan}" style="vertical-align:middle"><pre>${family.num_stripes}</pre></td>
  <td rowspan="${familyRowSpan}" style="vertical-align:middle"><pre>${family.num_sub_stripes}</pre></td>
  <th rowspan="${familyRowSpan}" style="vertical-align:middle; border-right: solid 1px #dee2e6" scope="row"><pre>${family.min_replication_level}</pre></th>
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
    return ReplicationConfigCatalogs;
});
