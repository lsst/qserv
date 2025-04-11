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

    CSSLoader.load('qserv/css/QservWorkerResultsFilesystem.css');

    class QservWorkerResultsFilesystem extends FwkApplication {

        constructor(name) {
            super(name);
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
                    this._init();
                    this._load();
                }
            }
        }
        _init() {
            if (this._initialized === undefined) this._initialized = false;
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="row" id="fwk-qserv-results-filesystem-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-1">
        ${Common.html_update_ival('update-interval', 10)}
      </div>
      <div class="form-group col-md-1">
        <label for="reset-controls-form">&nbsp;</label>
        <button id="reset-controls-form" class="btn btn-primary form-control">Reset</button>
      </div>
    </div>
  </div>
</div>
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover table-bordered" id="fwk-qserv-results-filesystem">
      <thead class="thead-light">
        <tr>
          <th class="sticky">worker</th>
          <th class="sticky">folder</th>
          <th class="sticky" style="text-align:right;">capacity [GB]</th>
          <th class="sticky" style="text-align:right;">free [GB]</th>
          <th class="sticky" style="text-align:right;">available [GB]</th>
          <th class="sticky" style="text-align:right;">used [%]</th>
          <th class="sticky" style="text-align:right;">#files</th>
          <th class="sticky" style="text-align:right;">size [GB]</th>
        </tr>
      </thead>
      <caption class="updating">Loading...</caption>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            cont.find(".form-control-selector").change(() => {
                this._load();
            });
            cont.find("button#reset-controls-form").click(() => {
                this._set_update_interval_sec(10);
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

        /**
         * Table for displaying info on MySQL connections that are being open at workers.
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-qserv-results-filesystem');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web service then render it to the application's page.
         */
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');

            Fwk.web_service_GET(
                "/replication/qserv/worker/status",
                {timeout_sec: 2, version: Common.RestAPIVersion},
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

        /**
         * Display MySQL connections
         */
        _display(data) {
            const workerFilesInspectTitle = "Click to see files existing on the worker.";
            let html = '';
            for (let worker in data) {
                if (!data[worker].success) {
                    html += `
<tr>
  <th class="table-warning">${worker}</th>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
  <td class="table-secondary">&nbsp;</td>
</tr>`;
                } else {
                    let filesystem = data[worker].info.filesystem;
                    const used = (filesystem.capacity_bytes > 0 && filesystem.available_bytes > 0) ?
                        (100.0 * (filesystem.capacity_bytes - filesystem.available_bytes) / filesystem.capacity_bytes).toFixed(1) :
                        -1;
                    html += `
<tr worker="${worker}" class="display-worker-files" title="${workerFilesInspectTitle}">
  <th>${worker}</th>
  <td>${filesystem.folder}</td>
  <td style="text-align:right;"><pre>${QservWorkerResultsFilesystem._bytes2gb(filesystem.capacity_bytes)}</pre></td>
  <td style="text-align:right;"><pre>${QservWorkerResultsFilesystem._bytes2gb(filesystem.free_bytes)}</pre></td>
  <td style="text-align:right;"><pre>${QservWorkerResultsFilesystem._bytes2gb(filesystem.available_bytes)}</pre></td>
  <td style="text-align:right;"><pre>${used}</pre></td>
  <td style="text-align:right;"><pre>${filesystem.num_result_files}</pre></td>
  <td style="text-align:right;"><pre>${QservWorkerResultsFilesystem._bytes2gb(filesystem.size_result_files_bytes)}</pre></td>
</tr>`;
                }
            }
            let tbody = this._table().children('tbody').html(html);
            let displayWorkerFiles  = function(e) {
                const worker = $(e.currentTarget).attr("worker");
                Fwk.find("Workers", "Files").set_worker(worker);
                Fwk.show("Workers", "Files");
            };
            tbody.find("tr.display-worker-files").click(displayWorkerFiles);
        }
        static _GiB = 1024 * 1024 * 1024;
        static _bytes2gb(bytes) {
            return bytes < 0 ? bytes : (bytes / QservWorkerResultsFilesystem._GiB).toFixed(1);
        }
    }
    return QservWorkerResultsFilesystem;
});
