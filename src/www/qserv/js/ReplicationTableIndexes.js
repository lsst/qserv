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

    CSSLoader.load('qserv/css/ReplicationTableIndexes.css');

    class ReplicationTableIndexes extends FwkApplication {

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

        loadIndexes(database, table) {
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
    <label for="indexes-database">Database:</label>
    <select id="indexes-database" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-3">
    <label for="indexes-table">Table:</label>
    <select id="indexes-table" class="form-control form-control-view">
    </select>
  </div>
  <div class="form-group col-md-1">
    <label for="indexes-overlap">Overlap </i></label>
    <select id="indexes-overlap" class="form-control form-control-view">
      <option value="0" selected>No</option>
      <option value="1">Yes</option>
    </select>
  </div>
  <div class="form-group col-md-1">
    ${Common.html_update_ival('update-interval', 600)}
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-replication-indexes">
      <thead class="thead-light">
        <tr>
          <th rowspan="2">index</th>
          <th rowspan="2">unique</th>
          <th rowspan="2">type</th>
          <th rowspan="2">status</th>
          <th rowspan="2">#replicas</th>
          <th rowspan="2">comment</th>
          <th colspan="4">columns</th>
        </tr>
        <tr>
          <th>name</th>
          <th>seq</th>
          <th>sub_part</th>
          <th>collation</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find("#indexes-database").change(() => {
                this._set_tables(this._get_database());
                this._load_indexes();
            });
            cont.find("#indexes-table").change(() => {
                this._load_indexes();
            });
            cont.find("#indexes-overlap").change(() => {
                this._load_indexes();
            });
            cont.find("#update-interval").change(() => {
                this._load();
            });
            this._disable_selectors(true);
        }

        /**
         * The body of the HTML table for displaying indexes of the selected MySQL table.
         * @returns JQuery table object
         */
        _indexes() {
            if (this._indexes_obj === undefined) {
                this._indexes_obj = this.fwk_app_container.find('table#fwk-replication-indexes');
            }
            return this._indexes_obj;
        }
        _status() {
            if (this._status_obj === undefined) {
                this._status_obj = this._indexes().children('caption');
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
        _get_database() { return this._form_control('select', 'indexes-database').val(); }
        _set_database(val) { this._form_control('select', 'indexes-database').val(val); }
        _get_table() { return this._form_control('select', 'indexes-table').val(); }
        _set_table(val) { this._form_control('select', 'indexes-table').val(val); }
        _get_overlap() { return this._form_control('select', 'indexes-overlap').val(); }

        _set_databases(databases) {
            this._databases = databases;
            let html = '';
            _.each(this._databases, (tables, database) => {
                const selected = !html ? 'selected' : ''; 
                html += `<option value="${database}" ${selected}>${database}</option>`;

            });
            this._form_control('select', 'indexes-database').html(html);
            if (this._current_database) this._set_database(this._current_database);
            this._set_tables(this._get_database());
        }
        _set_tables(database) {
            let html = '';
            _.each(this._databases[database], (table) => {
                const selected = !html ? 'selected' : ''; 
                html += `<option value="${table}" ${selected}>${table}</option>`;
            });
            this._form_control('select', 'indexes-table').html(html);
            if (this._current_database && this._current_table && (database === this._current_database)) {
                this._set_table(this._current_table);                
            }
        }
        _disable_selectors(disable) {
            this._form_control('select', 'indexes-database').prop('disabled', disable);
            this._form_control('select', 'indexes-table').prop('disabled', disable);
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
                this._indexes().children('tbody').html('');
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
                    this._load_indexes();
                },
                (msg) => { this._end_load(msg); }
            );
        }
        _load_indexes() {
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
                "/replication/sql/index/" + this._current_database + "/" + this._current_table,
                {overlap: this._get_overlap(), version: Common.RestAPIVersion},
                (data) => {
                    if (!data.success) {
                        this._end_load(data.error);
                        return;
                    }
                    this._end_load();
                    this._display(data.status.indexes);
                },
                (msg) => { this._end_load(msg); }
            );
        }
        _display(indexes) {
            let html = '';
            for (let i in indexes) {
                let index = indexes[i];
                let rowspan = index.columns.length + 1;
                html += `
<tr>
  <th rowspan="${rowspan}" style="vertical-align:top"><pre>${index.name}</pre></th>
  <td rowspan="${rowspan}" style="vertical-align:top"><pre>${index.unique ? 'YES' : 'NO'}</pre></td>
  <th rowspan="${rowspan}" style="vertical-align:top"><pre>${index.type}</pre></th>
  <td rowspan="${rowspan}" style="vertical-align:top"><pre class="${this._status2class(index.status)}">${index.status}</pre></td>
  <th rowspan="${rowspan}" style="vertical-align:top"><pre>${index.num_replicas} / ${index.num_replicas_total}</pre></th>
  <td rowspan="${rowspan}" style="vertical-align:top; padding-top:3px"id="${i}"></td>
</tr>`;
                for (let j in index.columns) {
                    let column = index.columns[j];
                    html += `
<tr>
  <th><pre>${column.name}</pre></th>
  <td><pre>${column.seq}</pre></td>
  <td><pre>${column.sub_part}</pre></td>
  <td><pre>${column.collation}</pre></td>
</tr>`;
                }
            }
            let tbody = this._indexes().children('tbody');
            tbody.html(html);
            // Comments are set via DOM to avoid failures that may arrise during
            // static HTML generation due to special HTML tags or markup symbols
            // like '<', '>', etc.
            for (let i in indexes) {
                tbody.find('td#' + i).text(indexes[i].comment);
            }
        }
        _status2class(status) {
            if (status === "COMPLETE") return "status-complete";
            else if (status === "INCOMPLETE") return "status-incomplete";
            return "status-INCONSISTENT";
        }
    }
    return ReplicationTableIndexes;
});

