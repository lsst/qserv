define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/ReplicationController.css');

    class ReplicationController extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */ 
        static update_ival_sec() { return 10; }

        constructor(name) {
            super(name);
            this._prev_update_sec = 0;  // for triggering page updates
            this._prevTimestamp = 0;    // for incremental pull of the logged events
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_show
         */
        fwk_app_on_show() {
            console.log('show: ' + this.fwk_app_name);
            this.fwk_app_on_update();
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_hide
         */
        fwk_app_on_hide() {
            console.log('hide: ' + this.fwk_app_name);
        }

        /**
         * Override event handler defined in the base class
         * @see FwkApplication.fwk_app_on_update
         */
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > ReplicationController.update_ival_sec()) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
                }
            }
        }

        /**
         * The one time initialization of the page's layout
         */
        _init() {
            if (!_.isUndefined(this._initialized)) return;
            this._initialized = true;

            let html = `
<div class="row">
  <div class="col">
    <h3>Status</h3>
    <table class="table table-sm table-hover" id="fwk-controller-status">
      <caption class="updating">
        Loading...
      </caption>
      <tbody></tbody>
    </table>
  </div>
</div>
<div class="row">
  <div class="col">
    <h3>Search event log <button id="reset-events-form" class="btn btn-primary">Reset</button></h3>
    <div class="form-row">
      <div class="form-group col-md-2">
        <label for="current-controller">Controller:</label>
        <select id="current-controller" class="form-control">
          <option value="0" selected>any</option>
          <option value="1">CURRENT</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="task">Task:</label>
        <select id="task" class="form-control">
          <option value="" selected>any</option>
        </select>
      </div>
      <div class="form-group col-md-3">
        <label for="operation">Operation:</label>
        <select id="operation" class="form-control">
          <option value="" selected>any</option>
        </select>
      </div>
      <div class="form-group col-md-2">
        <label for="status">Status:</label>
        <select id="status" class="form-control">
          <option value="" selected>any</option>
        </select>
      </div>
      <div class="form-group col-md-1">
        <label for="max-events">Max.events:</label>
        <select id="max-events" class="form-control">
          <option value="100" selected>100</option>
          <option value="200">200</option>
          <option value="500">500</option>
          <option value="1000">1,000</option>
          <option value="2000"2,000</option>
        </select>
      </div>
    </div>
    <table class="table table-sm table-hover" id="fwk-controller-log">
      <thead class="thead-light">
        <tr>
          <th rowspan="2">Timestamp</th>
          <th rowspan="2">Task</th>
          <th rowspan="2">Operation</th>
          <th rowspan="2">Status</th>
          <th colspan="2">Event Details</th>
        </tr>
        <tr>
          <th>flag</th>
          <th>value</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>`;
            let cont = this.fwk_app_container.html(html);
            // Note that the table gets reset after making changes to the selector
            // to prevent seeing a potentially confusing mixture of the events.
            cont.find(".form-control").change(() => {
                this._loadFromScratch();
            });
            cont.find("button#reset-events-form").click(() => {
                this._set_current_controller("0");
                this._set_task("");
                this._set_operation("");
                this._set_operation_status("");
                this._set_max_events("100");
                this._loadFromScratch();
            });
        }
        
        /**
         * Table for displaying the general status of the Master Replication Controller
         * @returns JQuery table object
         */
        _tableStatus() {
            if (this._tableStatus_obj === undefined) {
                this._tableStatus_obj = this.fwk_app_container.find('table#fwk-controller-status');
            }
            return this._tableStatus_obj;
        }

        /**
         * Table for displaying event log of the current Master Replication Controller
         * @returns JQuery table object
         */
        _tableLog() {
            if (this._tableLog_obj === undefined) {
                this._tableLog_obj = this.fwk_app_container.find('table#fwk-controller-log');
            }
            return this._tableLog_obj;
        }
        _form_control(elem_type, id) {
            if (this._form_control_obj === undefined) this._form_control_obj = {};
            if (!_.has(this._form_control_obj, id)) {
                this._form_control_obj[id] = this.fwk_app_container.find(elem_type + '#' + id);
            }
            return this._form_control_obj[id];
        }
        _get_task() { return this._form_control('select', 'task').val(); }
        _set_task(val) { this._form_control('select', 'task').val(val); }

        _get_operation() { return this._form_control('select', 'operation').val(); }
        _set_operation(val) { this._form_control('select', 'operation').val(val); }

        _get_operation_status() { return this._form_control('select', 'status').val(); }
        _set_operation_status(val) { this._form_control('select', 'status').val(val); }

        _get_max_events() { return this._form_control('select', 'max-events').val(); }
        _set_max_events(val) { this._form_control('select', 'max-events').val(val); }

        _get_current_controller() { return this._form_control('select', 'current-controller').val(); }
        _set_current_controller(val) { this._form_control('select', 'current-controller').val(val); }

        _isImportant(event) {
            if (this._importantKeys === undefined) {
                this._importantKeys = [
                    'failed-qserv-worker'
                ];
            }
            for (let i in event.kv_info) {
                let kv = event.kv_info[i];
                for (let k in kv) {
                    if (_.contains(this._importantKeys, k)) return true;
                }
            }
            return false;
        }

        /**
         * This method should be called instead of _log() after making changes to the selectors
         * in order to prevent seeing a potentially confusing mixture of events.
         */
        _loadFromScratch() {
            this._prevTimestamp = 0;
            this._tableLog().children('tbody').html("");
            this._load();
        }

        /**
         * Load data from the REST services then update the application's page.
         */
        _load() {
            if (!this._loadStarted()) return;
            this._loadControllerInfo((controllerId) => {
                this._loadApiVersion(() => {
                    this._loadLogDictionary(controllerId, () => {
                        this._loadLogEvents(controllerId, () => {
                            this._loadFinished();
                        });
                    });
                });
            });
        }
        _loadStarted() {
            // Prevent queueing multiple long requests if for some reason the server
            // is unable to address them within the refresh interval set for the application.
            if (_.isUndefined(this._loading) || !this._loading) {
                this._loading = true;
                this._tableStatus().children('caption').addClass('updating');
                return true;
            }
            return false;
        }

        _loadFailed(msg) {
            console.log('request failed', this.fwk_app_name, msg);
            this._tableStatus().children('caption').html('<span style="color:maroon">No Response</span>');
            this._tableStatus().children('caption').removeClass('updating');
            this._loading = false;
        }

        _loadFinished() {
            Fwk.setLastUpdate(this._tableStatus().children('caption'));
            this._tableStatus().children('caption').removeClass('updating');
            this._loading = false;
        }

        _loadControllerInfo(onLoaded) {
            Fwk.web_service_GET(
                "/replication/controller",
                {   "current_only": true
                },
                (data) => {
                    let controller = undefined;
                    switch (data.controllers.length) {
                        case 0: break;
                        case 1:
                            // Only one controller is supposed to be returned by the latest version
                            // of the service that recognizes the option "current_only". Or, it could
                            // be the only known controller.
                            controller = data.controllers[0];
                            break;
                        default:
                            // This is done if the service has an older version that doesn't recognize
                            // the option "current_only". Then scan the array to find the right one.
                            controller = _.find(data.controllers, function(controller) { return controller.current; });
                            break;
                    }
                    if (_.isUndefined(controller)) {
                        this._loadFailed("no Master Controller has been found.");
                        return;
                    }
                    this._displayStatus(controller);
                    onLoaded(controller.id);
                },
                (msg) => { this._loadFailed(msg); }
            );
        }

        _loadLogDictionary(controllerId, onLoaded) {
            // The dictionary (if the corresponding REST service is available) is loaded just once.
            if (!_.isUndefined(this._dict)) {
                onLoaded();
                return;
            }

            // The dictionary is optional. If none is availble then no dictionary-based event filtering
            // will be offered to a user. Any errors encountered during the loading attempt will be
            // reported to the Console for debugging purposes only. Then the loding chain will
            // get resumed.
            this._dict = {};
            Fwk.web_service_GET(
                "/replication/controller/" + controllerId + "/dict",
                {   "log_current_controller": 0,
                },
                (data) => {
                    if (data.success === 0) {
                        console.log("/replication/controller/:id/dict failed, error:", data.error, data.error_ext);
                    } else {

                        this._dict = data.log_dict;

                        // Update selectors
                        let html = `<option value="" selected>any</option>`;
                        for (let i in this._dict["task"]) {
                            const task = this._dict["task"][i];
                            html += `<option value="${task}">${task}</option>`;
                        }
                        this.fwk_app_container.find('select#task').html(html);

                        html = `<option value="" selected>any</option>`;
                        for (let i in this._dict["operation"]) {
                            const op = this._dict["operation"][i];
                            html += `<option value="${op}">${op}</option>`;
                        }
                        this.fwk_app_container.find('select#operation').html(html);

                        html = `<option value="" selected>any</option>`;
                        for (let i in this._dict["status"]) {
                            const st = this._dict["status"][i];
                            html += `<option value="${st}">${st}</option>`;
                        }
                        this.fwk_app_container.find('select#status').html(html);
                    }
                    onLoaded();
                },
                (msg) => {
                    console.log("/replication/controller/:id/dict failed, error:", msg);
                    onLoaded();
                }
            );
        }

        _loadApiVersion(onLoaded) {
            Fwk.web_service_GET(
                "/meta/version",
                {},
                (data) => {
                    this._tableStatus().find("pre#api-version").text(data.version);
                    onLoaded();
                },
                (msg) => {
                    this._loadFailed(msg);
                }
            );
        }

        _loadLogEvents(controllerId, onLoaded) {
            Fwk.web_service_GET(
                "/replication/controller/" + controllerId,
                {   "log": 1,
                    "log_current_controller": this._get_current_controller(),
                    "log_task": this._get_task(),
                    "log_operation": this._get_operation(),
                    "log_operation_status": this._get_operation_status(),
                    "log_from": this._prevTimestamp + 1,     // 1ms later
                    "log_max_events": this._get_max_events()
                },
                (data) => {
                    if (data.log.length > 0) this._prevTimestamp = data.log[0].timestamp;
                    this._displayLog(data.log);
                    onLoaded();
                },
                (msg) => {
                    this._loadFailed(msg);
                }
            );
        }

        _displayStatus(controller) {
            let started = new Date(controller.start_time);
            let html = `
<tr>
  <th style="text-align:left" scope="row">Status</th>
  <td style="text-align:left"><pre>RUNNING</pre></td>
</tr>
<tr>
  <th style="text-align:left" scope="row">id</th>
  <td style="text-align:left"><pre>` + controller.id + `</pre></td>
</tr>
<tr>
  <th style="text-align:left" scope="row">Started</th>
  <td style="text-align:left"><pre>` + started.toLocalTimeString() + `</pre></td>
</tr>
<tr>
  <th style="text-align:left" scope="row">Host</th>
  <td style="text-align:left"><pre>` + controller.hostname + `</pre></td>
</tr>
<tr>
  <th style="text-align:left" scope="row">PID</th>
  <td style="text-align:left"><pre>` + controller.pid + `</pre></td>
</tr>
<tr>
  <th style="text-align:left" scope="row">API version</th>
  <td style="text-align:left"><pre id="api-version">Loading...</pre></td>
</tr>`;
            this._tableStatus().children('tbody').html(html);
        }

        _displayLog(log) {
            let html = '';
            for (let i in log) {
                let event = log[i];
                let warningCssClass = this._isImportant(event) ? 'class="table-warning"' : '' ;
                let timestamp = new Date(event.timestamp);
                let rowspanAttr = event.kv_info.length === 0
                    ? '' : 'rowspan="' + (event.kv_info.length + 1) + '"';
                html += `
<tr>
  <th ` + rowspanAttr + ` scope="row"><pre>` + timestamp.toISOString() + `</pre></th>
  <td ` + rowspanAttr + `><pre>` + event.task + `</pre></td>
  <td ` + rowspanAttr + `><pre>` + event.operation + `</pre></td>
  <td ` + rowspanAttr + `><pre>` + event.status + `</pre></td>
</tr>`;
                if (event.kv_info.length !== 0) {
                    for (let j in event.kv_info) {
                        let kv = event.kv_info[j];
                        for (let k in kv) {
                            html += `
<tr ` + warningCssClass + `>
  <th scope="row"><pre>` + k + `</pre></th>
  <td>` + kv[k] + `</td>
</tr>`;
                        }
                    }
                }
            }
            this._tableLog().children('tbody').html(html + this._tableLog().children('tbody').html());
        }
    }
    return ReplicationController;
});
