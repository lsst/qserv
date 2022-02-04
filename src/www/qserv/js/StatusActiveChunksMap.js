define([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkApplication,
         _) {

    CSSLoader.load('qserv/css/StatusActiveChunksMap.css');

    class StatusActiveChunksMap extends FwkApplication {

        /**
         * @returns the default update interval for the page
         */ 
        static update_ival_sec() { return 1; }

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
                if (now_sec - this._prev_update_sec > StatusActiveChunksMap.update_ival_sec()) {
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

            this._WIDTH    = 1024;              /* window.innerWidth  -  60 */
            this._HEIGHT   =  660;              /* window.innerHeight - 140 */
            this._CENTER_X = this._WIDTH  / 2;
            this._CENTER_Y = this._HEIGHT / 2;
            this._SCALE_X  = this._WIDTH  / 4;
            this._SCALE_Y  = this._HEIGHT / 2;

            this._scheduler2color = {
                Snail:  '#007bff',
                Slow:   '#17a2b8',
                Med:    '#28a745',
                Fast:   '#ffc107',
                Group:  '#dc3545',
                Other:  'maroon'
            };

            let html = `
<div class="row">
  <div class="col">
    <canvas id="fwk-status-activechunksmap" width="`+this._WIDTH+`" height="`+this._HEIGHT+`"></canvas>
  </div>
  <div class="col">
    <table class="table table-sm table-hover table-borderless" id="fwk-status-activechunksmap-table">
      <caption class="updating">Loading...</caption>
      <tbody>
        <tr>
          <th>Projection:</th>
          <td colspan="3">
            <select id="projection">
              <option value="Hammer-Aitoff" selected>Hammer-Aitoff</option>
              <option value="Mollweide"     disabled>Mollweide</option>
            </select>
          </td>
        </tr>
        <tr>
          <th>Schedulers</th>
          <th class="right-aligned">#chunks</th>
          <th class="right-aligned">#tasks</th>
          <th class="right-aligned">#queued</th>
        </tr>
        <tr>
          <td class="bg-primary">Snail</td>
          <td><pre class="right-aligned chunks"          id="Snail"></pre></td>
          <td><pre class="right-aligned in-flight-tasks" id="Snail"></pre></td>
          <td><pre class="right-aligned queued-tasks"    id="Snail"></pre></td>
        </tr>
        <tr>
          <td class="bg-info">Slow</td>
          <td><pre class="right-aligned chunks"          id="Slow"></pre></td>
          <td><pre class="right-aligned in-flight-tasks" id="Slow"></pre></td>
          <td><pre class="right-aligned queued-tasks"    id="Slow"></pre></td>
        </tr>
        <tr>
          <td class="bg-success">Med</td>
          <td><pre class="right-aligned chunks"          id="Med"></pre></td>
          <td><pre class="right-aligned in-flight-tasks" id="Med"></pre></td>
          <td><pre class="right-aligned queued-tasks"    id="Med"></pre></td>
        </tr>
        <tr>
          <td class="bg-warning">Fast</td>
          <td><pre class="right-aligned chunks"          id="Fast"></pre></td>
          <td><pre class="right-aligned in-flight-tasks" id="Fast"></pre></td>
          <td><pre class="right-aligned queued-tasks"    id="Fast"></pre></td>
        </tr>
        <tr>
          <td class="bg-danger">Group</td>
          <td><pre class="right-aligned chunks"          id="Group"></pre></td>
          <td><pre class="right-aligned in-flight-tasks" id="Group"></pre></td>
          <td><pre class="right-aligned queued-tasks"    id="Group"></pre></td>
        </tr>
      </tbody>
    </table>
  </div>
</div>`;
            this.fwk_app_container.html(html);
        }

        /**
         * @returns JQuery object for the 2D drawing context
         */
        _context() {
            if (this._context_obj === undefined) {
                this._context_obj = this.fwk_app_container.find('canvas#fwk-status-activechunksmap')[0].getContext('2d');
            }
            return this._context_obj;
        }
        
        /**
         * @returns JQuery object for the table
         */
        _table() {
            if (this._table_obj === undefined) {
                this._table_obj = this.fwk_app_container.find('#fwk-status-activechunksmap-table');
            }
            return this._table_obj;
        }

        /**
         * Load data from a web servie then render it to the application's
         * page.
         */
        _load() {

            if (this._loading === undefined) {
                this._loading = false;
            }
            if (this._loading) return;
            this._loading = true;

            this._table().children('caption').addClass('updating');

            Fwk.web_service_GET(
                "/replication/qserv/worker/status",
                {'timeout_sec': 2},
                (data) => {
                    this._display(data.chunks, data.schedulers_to_chunks, data.status);
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

        _display(chunks, schedulers_to_chunks, status) {

            let c = this._context();

            /*
             * The coordinate system
             */

            c.clearRect(0, 0, this._WIDTH, this._HEIGHT);
            c.lineWidth = 0.75;

            c.strokeStyle = 'grey';
            c.beginPath();
            c.moveTo(0,           this._CENTER_Y);
            c.lineTo(this._WIDTH, this._CENTER_Y);
            /*
            c.moveTo(this._CENTER_X, 0);
            c.lineTo(this._CENTER_X, this._HEIGHT);
            */
            c.stroke();
            c.strokeStyle = 'lightgrey';

            let lon_min = -180.,
                lon_max =  180.,
                lat_min =  -90.,
                lat_max =   90.;

            let prev_coord = undefined;

            for (let lat = lat_min; lat <= lat_max; lat += 5.) {
                for (let lon = lon_min; lon <= lon_max; lon += 10.) {

                    let coord = this._xy(lon, lat);

                    c.beginPath();
                    c.moveTo(coord.x - 1, coord.y);
                    c.lineTo(coord.x + 1, coord.y);
                    c.stroke();

                    if (!_.isUndefined(prev_coord)) {
                        c.beginPath();
                        c.moveTo(coord.x,      coord.y);
                        c.lineTo(prev_coord.x, prev_coord.y);
                        c.stroke();
                    }
                    prev_coord = coord;
                }
                prev_coord = undefined;
            }
            for (let lon = lon_min; lon <= lon_max; lon += 10.) {
                for (let lat = lat_min + 5.; lat < lat_max; lat += 5.) {

                    let coord = this._xy(lon, lat);

                    c.beginPath();
                    c.moveTo(coord.x - 1, coord.y);
                    c.lineTo(coord.x + 1, coord.y);
                    c.stroke();

                    if (!_.isUndefined(prev_coord)) {
                        c.beginPath();
                        c.moveTo(coord.x,      coord.y);
                        c.lineTo(prev_coord.x, prev_coord.y);
                        c.stroke();
                    }
                    prev_coord = coord;
                }
                prev_coord = undefined;
            }

            /*
             * Active chunks
             */
            this._table().find('.chunks').html('0');
            this._table().find('.in-flight-tasks').html('0');
            this._table().find('.queued-tasks').html('0');

            for (let scheduler in schedulers_to_chunks) {

                let name = scheduler.substring('Sched'.length);
                let color = _.has(this._scheduler2color, name) ?
                    this._scheduler2color[name] :
                    this._scheduler2color['Other'];

                c.fillStyle = color;

                for (let i in schedulers_to_chunks[scheduler]) {
                    let chunkId = schedulers_to_chunks[scheduler][i];
                    if (!_.has(chunks, chunkId)) {
                        console.log("StatusActiveChunksMap._display()  no info for chunkId=" + chunkId);
                        continue;
                    }
                    let chunk = chunks[chunkId].production;
                    let coord = this._xy((chunk.lon_min + chunk.lon_max) / 2, (chunk.lat_min + chunk.lat_max) / 2);
                    c.beginPath();
                    c.arc(coord.x, coord.y, 5, 0, Math.PI * 2, true);
                    c.fill();
                }
                this._table().find(`.chunks#${name}`).html(schedulers_to_chunks[scheduler].length);

                /*
                 * Count and display the total numbers of the active and queued tasks across
                 * all workers
                 */
                let inFlightTasks = 0;
                let queuedTasks = 0;
                for (let worker in status) {
                    // Consider contributions from the responded workers only
                    if (!status[worker].success) continue;
                    for (let i in status[worker].info.processor.queries.blend_scheduler.schedulers) {
                        let schedulerInfo = status[worker].info.processor.queries.blend_scheduler.schedulers[i];
                        if (schedulerInfo.name == scheduler) {
                            inFlightTasks += schedulerInfo.num_tasks_in_flight;
                            queuedTasks += schedulerInfo.num_tasks_in_queue;
                            break;
                        }
                    }
                }
                this._table().find(`.in-flight-tasks#${name}`).html(inFlightTasks);
                this._table().find(`.queued-tasks#${name}`).html(queuedTasks);
            }
        }
        
        /**
         * Translate angular coordinates of a point into coordinates on the canvas
         *
         * @param {Number} lon  for the angular longitude (degrees) 
         * @param {Number} lat  for the angular latitude (degrees)
         * @returns {Object} of two kyes {'x','y'} for the corresponding coordinates on the projection
         */
        _xy(lon, lat) {
            if (lon > 180) lon = lon - 360.;
            let rad_lon = lon * Math.PI / 180.;
            let rad_lat = lat * Math.PI / 180.;
            let common_denominator = Math.sqrt(1. + Math.cos(rad_lat) * Math.cos(rad_lon / 2.));
            let x = 2. * Math.cos(rad_lat) * Math.sin(rad_lon / 2.) / common_denominator;
            let y =      Math.sin(rad_lat)                          / common_denominator;

            let coord_x = this._CENTER_X + this._SCALE_X * x,
                coord_y = this._CENTER_Y - this._SCALE_Y * y;

            return {x: coord_x, y: coord_y};
        }
    }
    return StatusActiveChunksMap;
});
