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

    CSSLoader.load('qserv/css/ReplicationSchema.css');

    class ReplicationSchema extends FwkApplication {

        /**
         * @returns the default update interval for the page.
         */ 
        static update_ival_sec() { return 30; }

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

        loadSchema(database, table) {
            this._init();
            this._current_database = database;
            this._current_table = table;
            this._load();
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
<div class="form-row">
  <div class="form-group col-md-3">
    <label for="schema-database">Database:</label>
    <select id="schema-database" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-3">
    <label for="schema-table">Table:</label>
    <select id="schema-table" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-1">
    ${Common.html_update_ival('update-interval')}
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-replication-schema">
      <thead class="thead-light">
        <tr>
          <th class="sticky">position</th>
          <th class="sticky">column</th>
          <th class="sticky">type</th>
          <th class="sticky">is nullable</th>
          <th class="sticky">default</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find("#schema-database").change(() => {
                this._set_tables(this._get_database());
                this._load_schema();
            });
            cont.find("#schema-table").change(() => {
                this._load_schema();
            });
            cont.find("#update-interval").change(() => {
                this._load();
            });
            this._disable_selectors(true);
        }

        /**
         * Table for displaying schema of the selected table.
         * @returns JQuery table object
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-replication-schema');
            }
            return this._table_obj;
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this._table().children('caption');
            }
            return this._status_obj;
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _update_interval_sec() { return this._form_control('select', 'update-interval').val(); }
        _get_database() { return this._form_control('select', 'schema-database').val(); }
        _set_database(val) { this._form_control('select', 'schema-database').val(val); }
        _get_table() { return this._form_control('select', 'schema-table').val(); }
        _set_table(val) { this._form_control('select', 'schema-table').val(val); }

        _set_databases(databases) {
            console.log("_set_databases()", "databases:", databases);
            this._databases = databases;
            let html = '';
            _.each(this._databases, (tables, database) => {
                console.log("_set_databases()", "database:", database, "tables:", tables);
                const selected = !html ? 'selected' : ''; 
                html += `<option value="${database}" ${selected}>${database}</option>`;

            });
            this._form_control('select', 'schema-database').html(html);
            if (this._current_database) this._set_database(this._current_database);
            this._set_tables(this._get_database());
        }
        _set_tables(database) {
            console.log("_set_tables()", "database:", database, "tables:", this._databases[database]);
            let html = '';
            _.each(this._databases[database], (table) => {
                console.log("_set_tables()", "table:", table);
                const selected = !html ? 'selected' : ''; 
                html += `<option value="${table}" ${selected}>${table}</option>`;
            });
            this._form_control('select', 'schema-table').html(html);
            if (this._current_database && this._current_table && (database === this._current_database)) {
                this._set_table(this._current_table);                
            }
        }
        _disable_selectors(disable) {
            this._form_control('select', 'schema-database').prop('disabled', disable);
            this._form_control('select', 'schema-table').prop('disabled', disable);
        }

        /// @returns 'true' if started loading, or 'false' if loading is already happening
        _begin_load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return false;
            this._loading = true;
            this._status().addClass('updating');
            this._disable_selectors(true);
            return true;
        }
        _end_load(msg) {
            if (!_.isUndefined(msg)) {
                this._status().html(`<span style="color:maroon">${msg}</span>`);
                this._table().html('');
            }
            this._status().removeClass('updating');
            Fwk.setLastUpdate(this._status());
            this._disable_selectors(false);
            this._loading = false;
        }
        _load() {
            if (!this._begin_load()) return;
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._end_load(data.error);
                        return;
                    }
                    let databases = {};
                    _.each(data.config.databases, (database) => {
                        databases[database.database] = _.map(database.tables, (table) => {
                            return table.name;
                        });
                    });
                    this._set_databases(databases);
                    this._end_load();
                    this._load_schema();
                },
                (msg) => { this._end_load(msg); }
            );
        }
        _load_schema() {
            this._current_database = this._get_database();
            this._current_table = this._get_table();
            if (!this._current_database) {
                this._end_load("No databases exist yet in this instance of Qserv");
                return;
            }
            if (!this._current_table) {
                this._end_load("No databases or tables exist yet in this instance of Qserv");
                return;
            }
            if (!this._begin_load()) return;
            Fwk.web_service_GET(
                "/replication/sql/table/schema/" + this._current_database + "/" + this._current_table,
                {version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._end_load(data.error);
                        return;
                    }
                    this._end_load();
                    this._display(data.schema[this._current_database][this._current_table]);
                },
                (msg) => { this._end_load(msg); }
            );
        }

        /**
         * Display the configuration.
         */
        _display(columns) {
            let html = '';
            for (let i in columns) {
                let coldef = columns[i];
                html += `
<tr>
  <td scope="row"><pre>${coldef.ORDINAL_POSITION}</pre></td>
  <th><pre>${coldef.COLUMN_NAME}</pre></th>
  <td><pre>${coldef.COLUMN_TYPE.toUpperCase()}</pre></td>
  <td><pre>${coldef.IS_NULLABLE}</pre></td>
  <td><pre>${coldef.COLUMN_DEFAULT}</pre></td>
</tr>`;
            }
            this._table().children('tbody').html(html);
        }
    }
    return ReplicationSchema;
});

