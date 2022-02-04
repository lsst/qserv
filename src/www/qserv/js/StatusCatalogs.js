define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],
function (CSSLoader,
        Fwk,
        FwkApplication,
        _) {

    CSSLoader.load('qserv/css/StatusCatalogs.css');

    class StatusCatalogs extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */
        static update_ival_sec() { return 10; }

        constructor(name) { super(name); }

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
                if (now_sec - this._prev_update_sec > StatusCatalogs.update_ival_sec()) {
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

            let html = `
<div class="row">
  <div class="col">
    <h3>Databases</h3>
    <table class="table table-sm table-hover table-bordered" id="fwk-status-catalogs-databases">
      <thead class="thead-light">
        <tr>
          <th              rowspan="4" > Database       </th>
          <th colspan="2"  rowspan="3" > #chunks        </th>
          <th colspan="20"             > Data&nbsp;[GB] </th>
        </tr>
        <tr>
          <th colspan="10"              > in unique chunks </th>
          <th colspan="10"              > in all replicas </th>
        </tr>
        <tr>
          <th colspan="3"              > chunks   </th>
          <th colspan="3"              > overlaps </th>
          <th colspan="3"              > regular  </th>
          <th              rowspan="2" > &sum;    </th>
          <th colspan="3"              > chunks   </th>
          <th colspan="3"              > overlaps </th>
          <th colspan="3"              > regular  </th>
          <th              rowspan="2" > &sum;    </th>
        </tr>
        <tr>
          <th> unique </th>
          <th> replicas </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <h3>Partitioned tables</h3>
    <table class="table table-sm table-hover table-bordered" id="fwk-status-catalogs-partitioned">
      <thead class="thead-light">
        <tr>
          <th              rowspan="4"  > Database       </th>
          <th              rowspan="4"  > Table          </th>
          <th colspan="3"  rowspan="3"  > #rows&nbsp;in  </th>
          <th colspan="14"              > Data&nbsp;[GB] </th>
        </tr>
        <tr>
          <th colspan="7"              > in unique chunks </th>
          <th colspan="7"              > in all replicas </th>
        </tr>
        <tr>
          <th colspan="3"              > chunks   </th>
          <th colspan="3"              > overlaps </th>
          <th              rowspan="2" > &sum;    </th>
          <th colspan="3"              > chunks   </th>
          <th colspan="3"              > overlaps </th>
          <th              rowspan="2" > &sum;    </th>
        </tr>
        <tr>
          <th> chunks   </th>
          <th> overlaps </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <h3>Regular tables</h3>
    <table class="table table-sm table-hover table-bordered" id="fwk-status-catalogs-regular">
      <thead class="thead-light">
        <tr>
          <th              rowspan="3" > Database       </th>
          <th              rowspan="3" > Table          </th>
          <th              rowspan="3" > #rows          </th>
          <th colspan="14"             > Data&nbsp;[GB] </th>
        </tr>
        <tr>
          <th colspan="3"> in unique tables </th>
          <th colspan="3"> in all replicas </th>
        </tr>
        <tr>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
          <th> data  </th>
          <th> index </th>
          <th> &sum; </th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            this.fwk_app_container.html(html);
        }

        /**
         * @returns JQuery table object
         */
        _databases() {
            if (this._databases_obj === undefined) {
                this._databases_obj = this.fwk_app_container.find('table#fwk-status-catalogs-databases');
            }
            return this._databases_obj;
        }

        /**
         * @returns JQuery table object
         */
        _partitionedTables() {
            if (this._partitionedTables_obj === undefined) {
                this._partitionedTables_obj = this.fwk_app_container.find('table#fwk-status-catalogs-partitioned');
            }
            return this._partitionedTables_obj;
        }

        /**
         * @returns JQuery table object
         */
        _regularTables() {
            if (this._regularTables_obj === undefined) {
                this._regularTables_obj = this.fwk_app_container.find('table#fwk-status-catalogs-regular');
            }
            return this._regularTables_obj;
        }

        /**
         * Load data from a web service then render it to the application's
         * page.
         */
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._databases().children('caption').addClass('updating');

            Fwk.web_service_GET(
                "/replication/catalogs",
                {},
                (data) => {
                    this._display(data.databases);
                    Fwk.setLastUpdate(this._databases().children('caption'));
                    this._databases().children('caption').removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._databases().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._databases().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }

        /**
         * @returns {String} the input number of bytes translated into GiB wied precision
         */
        static size2gb(bytes) {
            if (bytes === 0) return '0.0';
            let gb = bytes / 1e9;
            return gb < 0.1 ? '<0.1' : Number(gb).toFixed(1);
        }

        /**
         * @returns {String} the input number of bytes translated into TiB wied precision
         */
        static size2tb(bytes) {
            if (bytes === 0) return '0.0';
            let tb = bytes / 1e12;
            return tb < 0.1 ? '<0.1' : Number(tb).toFixed(1);
        }

        _display(databases) {
            let total_chunks_unique = 0;
            let total_chunks_with_replicas = 0;
            let total_data_unique_in_chunks_data = 0;
            let total_data_unique_in_chunks_index = 0;
            let total_data_unique_in_chunks = 0;
            let total_data_unique_in_overlaps_data = 0;
            let total_data_unique_in_overlaps_index = 0;
            let total_data_unique_in_overlaps = 0;
            let total_data_unique_in_regular_data = 0;
            let total_data_unique_in_regular_index = 0;
            let total_data_unique_in_regular = 0;
            let total_data_unique = 0;
            let total_data_with_replicas_in_chunks_data = 0;
            let total_data_with_replicas_in_chunks_index = 0;
            let total_data_with_replicas_in_chunks = 0;
            let total_data_with_replicas_in_overlaps_data = 0;
            let total_data_with_replicas_in_overlaps_index = 0;
            let total_data_with_replicas_in_overlaps = 0;
            let total_data_with_replicas_in_regular_data = 0;
            let total_data_with_replicas_in_regular_index = 0;
            let total_data_with_replicas_in_regular = 0;
            let total_data_with_replicas = 0;
            let html = '';
            for (let database in databases) {
                let dInfo = databases[database];
                let data_unique_in_chunks_data    = 0;
                let data_unique_in_chunks_index   = 0;
                let data_unique_in_overlaps_data  = 0;
                let data_unique_in_overlaps_index = 0;
                let data_unique_in_regular_data   = 0;
                let data_unique_in_regular_index  = 0;
                let data_with_replicas_in_chunks_data    = 0;
                let data_with_replicas_in_chunks_index   = 0;
                let data_with_replicas_in_overlaps_data  = 0;
                let data_with_replicas_in_overlaps_index = 0;
                let data_with_replicas_in_regular_data   = 0;
                let data_with_replicas_in_regular_index  = 0;
                for (let table in dInfo.tables) {
                    let tInfo = dInfo.tables[table];
                    if (tInfo.is_partitioned) {
                        data_unique_in_chunks_data    += tInfo.data.unique.in_chunks.data;
                        data_unique_in_chunks_index   += tInfo.data.unique.in_chunks.index;
                        data_unique_in_overlaps_data  += tInfo.data.unique.in_overlaps.data;
                        data_unique_in_overlaps_index += tInfo.data.unique.in_overlaps.index;
                        data_with_replicas_in_chunks_data    += tInfo.data.with_replicas.in_chunks.data;
                        data_with_replicas_in_chunks_index   += tInfo.data.with_replicas.in_chunks.index;
                        data_with_replicas_in_overlaps_data  += tInfo.data.with_replicas.in_overlaps.data;
                        data_with_replicas_in_overlaps_index += tInfo.data.with_replicas.in_overlaps.index;
                    } else {
                        data_unique_in_regular_data  += tInfo.data.unique.data;
                        data_unique_in_regular_index += tInfo.data.unique.index;
                        data_with_replicas_in_regular_data  += tInfo.data.with_replicas.data;
                        data_with_replicas_in_regular_index += tInfo.data.with_replicas.index;
                    }
                }
                let data_unique_in_chunks =
                    data_unique_in_chunks_data +
                    data_unique_in_chunks_index;
                let data_unique_in_overlaps =
                    data_unique_in_overlaps_data +
                    data_unique_in_overlaps_index;
                let data_unique_in_regular =
                    data_unique_in_regular_data +
                    data_unique_in_regular_index;
                let data_unique =
                    data_unique_in_chunks +
                    data_unique_in_overlaps +
                    data_unique_in_regular;
                let data_with_replicas_in_chunks =
                    data_with_replicas_in_chunks_data +
                    data_with_replicas_in_chunks_index;
                let data_with_replicas_in_overlaps =
                    data_with_replicas_in_overlaps_data +
                    data_with_replicas_in_overlaps_index;
                let data_with_replicas_in_regular =
                    data_with_replicas_in_regular_data +
                    data_with_replicas_in_regular_index;
                let data_with_replicas =
                    data_with_replicas_in_chunks +
                    data_with_replicas_in_overlaps +
                    data_with_replicas_in_regular;
                html += `
<tr>
  <td><pre>${database}</pre></td>
  <td              class="number"><pre>${dInfo.chunks.unique}</pre></td>
  <td              class="number" style="border-right:solid 1px #aaa"><pre>${dInfo.chunks.with_replicas}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_unique_in_chunks_data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_unique_in_chunks_index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_unique_in_chunks)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_unique_in_overlaps_data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_unique_in_overlaps_index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_unique_in_overlaps)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_unique_in_regular_data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_unique_in_regular_index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_unique_in_regular)}</pre></th>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_unique)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_chunks_data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_chunks_index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_chunks)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_overlaps_data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_overlaps_index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_overlaps)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_regular_data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_regular_index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(data_with_replicas_in_regular)}</pre></th>
  <th scope="row"  class="number"><pre>${StatusCatalogs.size2gb(data_with_replicas)}</pre></th>
</tr>`;
                 total_chunks_unique                        += dInfo.chunks.unique;
                 total_chunks_with_replicas                 += dInfo.chunks.with_replicas;
                 total_data_unique_in_chunks_data           += data_unique_in_chunks_data;
                 total_data_unique_in_chunks_index          += data_unique_in_chunks_index;
                 total_data_unique_in_chunks                += data_unique_in_chunks;
                 total_data_unique_in_overlaps_data         += data_unique_in_overlaps_data;
                 total_data_unique_in_overlaps_index        += data_unique_in_overlaps_index;
                 total_data_unique_in_overlaps              += data_unique_in_overlaps;
                 total_data_unique_in_regular_data          += data_unique_in_regular_data;
                 total_data_unique_in_regular_index         += data_unique_in_regular_index;
                 total_data_unique_in_regular               += data_unique_in_regular;
                 total_data_unique                          += data_unique;
                 total_data_with_replicas_in_chunks_data    += data_with_replicas_in_chunks_data;
                 total_data_with_replicas_in_chunks_index   += data_with_replicas_in_chunks_index;
                 total_data_with_replicas_in_chunks         += data_with_replicas_in_chunks;
                 total_data_with_replicas_in_overlaps_data  += data_with_replicas_in_overlaps_data;
                 total_data_with_replicas_in_overlaps_index += data_with_replicas_in_overlaps_index;
                 total_data_with_replicas_in_overlaps       += data_with_replicas_in_overlaps;
                 total_data_with_replicas_in_regular_data   += data_with_replicas_in_regular_data;
                 total_data_with_replicas_in_regular_index  += data_with_replicas_in_regular_index;
                 total_data_with_replicas_in_regular        += data_with_replicas_in_regular;
                 total_data_with_replicas                   += data_with_replicas;
            }
            html += `
<tr style="background-color:#eee;">
  <th scope="row"><pre>Total [TB for data]</pre></th>
  <th scope="row" class="number"><pre>${total_chunks_unique}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${total_chunks_with_replicas}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_unique_in_chunks_data)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_unique_in_chunks_index)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_unique_in_chunks)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_unique_in_overlaps_data)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_unique_in_overlaps_index)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_unique_in_overlaps)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_unique_in_regular_data)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_unique_in_regular_index)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_unique_in_regular)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_unique)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_chunks_data)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_chunks_index)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_chunks)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_overlaps_data)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_overlaps_index)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_overlaps)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_regular_data)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_regular_index)}</pre></th>
  <th scope="row" class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2tb(total_data_with_replicas_in_regular)}</pre></th>
  <th scope="row" class="number"><pre>${StatusCatalogs.size2tb(total_data_with_replicas)}</pre></th>
</tr>`;
            this._databases().children('tbody').html(html);

            html = '';
            for (let database in databases) {
                let dInfo = databases[database];
                let databaseRowSpan = 1;
                let databaseHtml = '';
                for (let table in dInfo.tables) {
                    let tInfo = dInfo.tables[table];
                    if (!tInfo.is_partitioned) continue;
                    databaseRowSpan++;
                    databaseHtml = `
<tr>
  <td><pre class="database_table" database="${database}" table="${table}">${table}</pre></td>
  <td              class="number"><pre>${tInfo.rows.in_chunks}</pre></td>
  <td              class="number"><pre>${tInfo.rows.in_overlaps}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${tInfo.rows.in_chunks + tInfo.rows.in_overlaps}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_chunks.data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_chunks.index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_chunks.data +
                                                                tInfo.data.unique.in_chunks.index)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_overlaps.data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_overlaps.index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_overlaps.data +
                                                                tInfo.data.unique.in_overlaps.index)}</pre></th>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.in_chunks.data +
                                                                tInfo.data.unique.in_chunks.index +
                                                                tInfo.data.unique.in_overlaps.data +
                                                                tInfo.data.unique.in_overlaps.index)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_chunks.data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_chunks.index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_chunks.data +
                                                                tInfo.data.with_replicas.in_chunks.index)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_overlaps.data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_overlaps.index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_overlaps.data +
                                                                tInfo.data.with_replicas.in_overlaps.index)}</pre></th>
  <th scope="row"  class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.in_chunks.data +
                                                                tInfo.data.with_replicas.in_chunks.index +
                                                                tInfo.data.with_replicas.in_overlaps.data +
                                                                tInfo.data.with_replicas.in_overlaps.index)}</pre></th>
</tr>` + databaseHtml;
                }
                if (databaseRowSpan > 1) {
                    html += `
<tr>
  <td rowspan="${databaseRowSpan}"><pre>${database}</pre></td>
</tr>` + databaseHtml;
                }
            }
            this._partitionedTables().children('tbody').html(html).find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });

            html = '';
            for (let database in databases) {
                let dInfo = databases[database];
                let databaseRowSpan = 1;
                let databaseHtml = '';
                for (let table in dInfo.tables) {
                    let tInfo = dInfo.tables[table];
                    if (tInfo.is_partitioned) continue;
                    databaseRowSpan++;
                    databaseHtml = `
<tr>
  <td><pre class="database_table" database="${database}" table="${table}">${table}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${tInfo.rows}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.index)}</pre></td>
  <th scope="row"  class="number" style="border-right:solid 1px #aaa"><pre>${StatusCatalogs.size2gb(tInfo.data.unique.data +
                                                                tInfo.data.unique.index)}</pre></th>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.data)}</pre></td>
  <td              class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.index)}</pre></td>
  <th scope="row"  class="number"><pre>${StatusCatalogs.size2gb(tInfo.data.with_replicas.data +
                                                                tInfo.data.with_replicas.index)}</pre></th>
</tr>` + databaseHtml;
                }
                if (databaseRowSpan > 1) {
                    html += `
<tr>
  <td rowspan="${databaseRowSpan}"><pre>${database}</pre></td>
</tr>` + databaseHtml;
                }
            }
            this._regularTables().children('tbody').html(html).find("pre.database_table").click((e) => {
                const elem = $(e.currentTarget);
                const database = elem.attr("database");
                const table = elem.attr("table");
                Fwk.show("Replication", "Schema");
                Fwk.current().loadSchema(database, table);
            });
        }
    }
    return StatusCatalogs;
});
