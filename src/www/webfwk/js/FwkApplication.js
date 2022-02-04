define([] ,

function() {

    /**
     * @brief The base class for user-defined applications.
     *
     * @returns {FwkApplication}
     */
    class FwkApplication {

        constructor(name) {
            this.fwk_app_name = name;
            this.fwk_app_container = null;
            this.fwk_app_visible = false;
        }

        show() {
            if (!this.fwk_app_visible) {
                this.fwk_app_visible = true;
                this.fwk_app_on_show();
            }
        }
        hide() {
            if (this.fwk_app_visible) {
                this.fwk_app_visible = false;
                this.fwk_app_on_hide();
            }
        }
        update() {
            this.fwk_app_on_update();
        }

        // These methods are supposed to be implemented by derived classes

        fwk_app_on_show()   { console.log('FwkApplication::fwk_app_on_show() NOT IMPLEMENTED'); }
        fwk_app_on_hide()   { console.log('FwkApplication::fwk_app_on_hide() NOT IMPLEMENTED'); }
        fwk_app_on_update() { console.log('FwkApplication::fwk_app_on_update() NOT IMPLEMENTED'); }
    }
    return FwkApplication;
});
