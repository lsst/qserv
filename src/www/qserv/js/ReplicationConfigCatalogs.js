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
    <table class="table table-sm table-hover" id="fwk-controller-config-catalogs">
      <thead class="thead-light">
        <tr>
          <th class="sticky">FAMILY</th>
          <th class="sticky">DATABASE</th>
          <th class="sticky">TABLE</th>
          <th class="sticky">type</th>
          <th class="sticky">director</th>
          <th class="sticky">director2</th>
          <th class="sticky">spatial</th>
          <th class="sticky">created</th>
          <th class="sticky">published</th>
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
                {version: Common.RestAPIVersion},
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

            const borderStyle = 'solid 1px #dee2e6';
            let html = '';
            for (let i in families) {
                let family = families[i];
                let familyRowSpan = 1;
                let familyHtml = '';
                for (let j in family['databases']) {
                    let database = family['databases'][j];
                    let databaseRowSpan = 1;
                    familyRowSpan += databaseRowSpan;
                    const databaseSchemaSupportCSS = `class="database_table" database="${database.database}" table=""`;
                    let databaseHtml = '';
                    for (let k in database.tables) {
                        let table = database.tables[k];
                        databaseRowSpan++;
                        familyRowSpan++;
                        const tableSchemaSupportCSS = `class="database_table" database="${database.database}" table="${table.name}"`;
                        let directorTableSchemaSupportCSS = "";
                        if(table.director_table !== "") {
                            const databaseName = table.director_database_name || database.database;
                            directorTableSchemaSupportCSS = `class="database_table" database="${databaseName}" table="${table.director_table_name}"`;
                        }
                        let directorTable2SchemaSupportCSS = "";
                        if (table.director_table2 !== "") {
                            const databaseName = table.director_database_name2 || database.database;
                            directorTable2SchemaSupportCSS = `class="database_table" database="${databaseName}" table="${table.director_table_name2}"`;
                        }
                        let type = "REG";
                        if (table.is_ref_match) {
                            type = "REF";
                        } else if (table.is_director) {
                            type = "DIR";
                        } else if (table.is_partitioned) {
                            type = "DEP";
                        }
                        databaseHtml += `
<tr ` + (k == database.tables.length - 1 ? ` style="border-bottom: ${borderStyle}"` : '') + `>
  <td scope="row">
    <span ${tableSchemaSupportCSS}>${table.name}</span>`;
                        if (table.is_ref_match) {
                            databaseHtml += `
      <br>ang_sep:&nbsp;<span style="font-weight:bold;">${table.ang_sep}</span>
      <br>flag:&nbsp;<span style="font-weight:bold;">${table.flag}</span>`;
                        } else if (table.is_director) {
                          databaseHtml += `
    <br>unique_primary_key:&nbsp;<span style="font-weight:bold;">${table.unique_primary_key}</span>`;
                        }
                        databaseHtml += `
  </td>
  <td style="border-right: ${borderStyle}"><span>${type}</span></td>`;
                        if (table.is_partitioned) {
                            if (table.is_ref_match) {
                                databaseHtml += `
  <td scope="row" style="border-right: ${borderStyle}">
    <span ${directorTableSchemaSupportCSS}>${table.director_database_name || database.database}</span><br>
    <span ${directorTableSchemaSupportCSS}>${table.director_table_name}</span></br>
    <span>${table.director_key}</span>
  </td>
  <td scope="row" style="border-right: ${borderStyle}">
    <span ${directorTable2SchemaSupportCSS}>${table.director_database_name2 || database.database}</span><br>
    <span ${directorTable2SchemaSupportCSS}>${table.director_table_name2}</span></br>
    <span>${table.director_key2}</span>
  </td>`;
                            } else if (table.is_director) {
                                databaseHtml += `
  <td scope="row" style="border-right: ${borderStyle}">
    <span>${table.director_key}</span>
  </td>
  <td style="border-right: ${borderStyle}">&nbsp;</td>`;
                            } else {
                                databaseHtml += `
  <td scope="row" style="border-right: ${borderStyle}">
    <span ${directorTableSchemaSupportCSS}>${table.director_table_name}</span></br>
    <span>${table.director_key}</span>
  </td>
  <td style="border-right: ${borderStyle}">&nbsp;</td>`;
                            }
                        } else {
                            databaseHtml += `
  <td style="border-right: ${borderStyle}">&nbsp;</td>
  <td style="border-right: ${borderStyle}">&nbsp;</td>`;
                        }
databaseHtml += `
  <td style="border-right: ${borderStyle}">
    <span>${table.latitude_key}</span><br>
    <span>${table.longitude_key}</span>
  </td>
  <td><pre class="timestamp">${(new Date(table.create_time)).toLocalTimeString('iso')}</pre></td>
  <td><pre class="timestamp">${table.is_published ? (new Date(table.publish_time)).toLocalTimeString('iso') : ''}</pre></td>
</tr>`;
                    }
                    familyHtml += `
<tr style="border-bottom: ${borderStyle}">
  <td rowspan="${databaseRowSpan}" style="border-right: ${borderStyle}">
  <span ${databaseSchemaSupportCSS}>${database.database}</span>
    <table class="table table-sm table-borderless compact">
      <tbody>
        <tr>
          <td>created:</td>
          <td><pre class="timestamp">${(new Date(database.create_time)).toLocalTimeString('iso')}</pre></td>
        </tr>
        <tr>
          <td>published:</td>
          <td><pre class="timestamp">${database.is_published ? (new Date(database.publish_time)).toLocalTimeString('iso') : ''}</pre></td>
        </tr>
      </tbody>
    </table>
  </td>
</tr>` + databaseHtml;
                }
                html += `
<tr style="border-bottom: ${borderStyle}">
  <td rowspan="${familyRowSpan}" style="border-right: ${borderStyle}" scope="row">
    <span style="font-weight:bold;">${family.name}</span>
    <table class="table table-sm table-borderless compact">
      <tbody>
        <tr>
          <td>stripes:</td>
          <th><span>${family.num_stripes}</span></th>
        </tr>
        <tr>
          <td>sub_stripes:</td>
          <th><span>${family.num_sub_stripes}</span></th>
        </tr>
        <tr>
          <td>overlap:</td>
          <th><span>${family.overlap}</span></th>
        </tr>
        <tr>
          <td>repl_level:</td>
          <th><span>${family.min_replication_level}</span></th>
        </tr>
      </tbody>
    </table>
  </td>
</tr>` + familyHtml;
            }
            this._table().children('tbody').html(html).find(".database_table").click((e) => {
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
