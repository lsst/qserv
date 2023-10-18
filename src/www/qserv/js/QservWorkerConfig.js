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

    CSSLoader.load('qserv/css/QservWorkerConfig.css');

    class QservWorkerConfig extends FwkApplication {

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
<div class="row" id="fwk-worker-config-controls">
  <div class="col">
    <div class="form-row">
      <div class="form-group col-md-3">
        <label for="worker">Worker:</label>
        <select id="worker" class="form-control form-control-selector">
        </select>
      </div>
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
    <table class="table table-sm table-hover table-bordered" id="fwk-worker-config">
      <thead class="thead-light">
        <tr>
          <th class="sticky">section</th>
          <th class="sticky">parameter</th>
          <th class="sticky">actual value</th>
          <th class="sticky">input value</th>
          <th class="sticky">notes</th>
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
        _worker() { return this._form_control('select', 'worker').val(); }
        _set_worker(val) { this._form_control('select', 'worker').val(val); }
        _set_workers(workers) {
            const prev_worker = this._worker();
            let html = '';
            for (let i in workers) {
                const worker = workers[i];
                const selected = (_.isEmpty(prev_worker) && (i === 0)) ||
                                 (!_.isEmpty(prev_worker) && (prev_worker === worker));
                html += `
 <option value="${worker}" ${selected ? "selected" : ""}>${worker}</option>`;
            }
            this._form_control('select', 'worker').html(html);
        }
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('table#fwk-worker-config');
            }
            return this._table_obj;
        }
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;
            this._table().children('caption').addClass('updating');
            Fwk.web_service_GET(
                "/replication/config",
                {version: Common.RestAPIVersion},
                (data) => {
                    let workers = [];
                    for (let i in data.config.workers) {
                        workers.push(data.config.workers[i].name);
                    }
                    this._set_workers(workers);
                    this._load_config();
                },
                (msg) => {
                    console.log('request failed', this.fwk_app_name, msg);
                    this._table().children('caption').html('<span style="color:maroon">No Response</span>');
                    this._table().children('caption').removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _load_config() {
            Fwk.web_service_GET(
                "/replication/qserv/worker/config/" + this._worker(),
                {   timeout_sec: 2, version: Common.RestAPIVersion
                },
                (data) => {
                    if (data.success) {
                        this._display(data.config);
                        Fwk.setLastUpdate(this._table().children('caption'));
                    } else {
                        console.log('request failed', this.fwk_app_name, data.error);
                        this._table().children('caption').html('<span style="color:maroon">' + data.error + '</span>');
                    }
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
        _display(config) {
            console.log(config);
            // This map represents a union of the section names (the key) and parameter
            // names (the value represented by an array) of the corresponding names
            // found in the input and actual collections.
            let joinedSectionsAndParams = {};
            _.each(_.union(_.keys(config.input), _.keys(config.actual)), function(section) {
                joinedSectionsAndParams[section] = _.union(
                    _.has(config.input, section) ? _.keys(config.input[section]) : [],
                    _.has(config.actual, section) ? _.keys(config.actual[section]) : []
                );
            });
            let html = '';
            for (const section in joinedSectionsAndParams) {
                let sectionRowSpan = 1;
                let htmlSection = '';
                for (const i in joinedSectionsAndParams[section]) {
                    const param = joinedSectionsAndParams[section][i];
                    sectionRowSpan++;
                    const hasActualValue = _.has(config.actual, section) && _.has(config.actual[section], param);
                    const actualValue = hasActualValue ? config.actual[section][param] : "";
                    const hasInputValue = _.has(config.input, section) && _.has(config.input[section], param);
                    const inputValue = hasInputValue ? config.input[section][param] : "";
                    let actualParamCssClass = 'bg-white';
                    let inputParamCssClass = 'bg-white';
                    let notes = "&nbsp;"
                    if (hasActualValue && !hasInputValue) {
                        notes = 'default';
                        actualParamCssClass = 'bg-info';
                    } else if (!hasActualValue && hasInputValue) {
                        notes = 'obsolete';
                        inputParamCssClass = 'bg-warning';
                    }
                    htmlSection += `
<tr>
  <th><pre>${param}</pre></th>
  <td class="${actualParamCssClass}"><pre>${actualValue}</pre></td>
  <td class="${inputParamCssClass}"><pre>${inputValue}</pre></td>
  <th>${notes}</th>
</tr>`;
                }
                html += `
<tr>
  <th rowspan="${sectionRowSpan}"><pre>${section}</pre></th>
</tr>` + htmlSection;
            }
            this._table().children('tbody').html(html);
        }
    }
    return QservWorkerConfig;
});
