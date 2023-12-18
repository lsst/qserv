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

    CSSLoader.load('qserv/css/QservCzarConfig.css');

    class QservCzarConfig extends FwkApplication {

        static update_ival_sec = 10;    /// The default update interval for the page.
        static czar_name = "default";   /// The name of Czar.

        constructor(name) {
            super(name);
        }
        fwk_app_on_show() {
            this.fwk_app_on_update();
        }
        fwk_app_on_hide() {}
        fwk_app_on_update() {
            if (this.fwk_app_visible) {
                if (this._prev_update_sec === undefined) {
                    this._prev_update_sec = 0;
                }
                let now_sec = Fwk.now().sec;
                if (now_sec - this._prev_update_sec > QservCzarConfig.update_ival_sec) {
                    this._prev_update_sec = now_sec;
                    this._init();
                    this._load();
                }
            }
        }
        _init() {
            if (this._initialized === undefined) {
                this._initialized = false;
            }
            if (this._initialized) return;
            this._initialized = true;
            let html = `
<div class="row">
  <div class="col">
    <table class="table table-sm table-hover" id="fwk-qserv-czar-config">
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
            this.fwk_app_container.html(html);
        }
        _table() {
            if (_.isUndefined(this._table_obj)) this._table_obj = this.fwk_app_container.find('table#fwk-qserv-czar-config');
            return this._table_obj;
        }
        _status() {
            if (_.isUndefined(this._status_obj)) this._status_obj = this._table().children('caption');
            return this._status_obj;
        }
        _load() {
            if (this._loading === undefined) this._loading = false;
            if (this._loading) return;
            this._loading = true;

            this._status().addClass('updating');
            Fwk.web_service_GET(
                "/replication/qserv/master/config/" + QservCzarConfig.czar_name,
                {   timeout_sec: 2,
                    version: Common.RestAPIVersion
                },
                (data) => {
                    this._display(data);
                    if (data.success) {
                        this._display(data.config);
                        Fwk.setLastUpdate(this._status());
                    } else {
                        console.log('request failed', this.fwk_app_name, data.error);
                        this._status().html('<span style="color:maroon">' + data.error + '</span>');
                    }
                    this._status().removeClass('updating');
                    this._loading = false;
                },
                (msg) => {
                    this._status().html('<span style="color:maroon">No Response</span>');
                    this._status().removeClass('updating');
                    this._status().removeClass('updating');
                    this._loading = false;
                }
            );
        }
        _display(config) {
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
    return QservCzarConfig;
});

