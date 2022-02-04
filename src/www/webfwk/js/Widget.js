define([
    'webfwk/Class',
    'underscore'],

function(Class) {

    /**
     * The exception class used by Widgets
     * 
     * @param String message
     * @returns {WidgetError}
     */
    function WidgetError(message) {
        this.message = message;
    }
    Class.define_class(WidgetError, Error, {}, {});

    function ASSERT(expression) {
        if (!expression) throw new WidgetError('Widget::'+arguments.callee.caller.name);
    }
    function PROP(obj, prop, default_val, validator) {
        if (_.has(obj, prop)) {
            var val = obj[prop];
            if (validator) ASSERT(validator(val));
            return val;
        }
        ASSERT(!_.isUndefined(default_val));
        return default_val;
    }
    function PROP_STRING(obj, prop, default_val) {
        return PROP(obj, prop, default_val, _.isString);
    }
    function PROP_NUMBER(obj, prop, default_val) {
        return PROP(obj, prop, default_val, _.isNumber);
    }
    function PROP_BOOL(obj, prop, default_val) {
        return PROP(obj, prop, default_val, _.isBoolean);
    }
    function PROP_FUNCTION(obj, prop, default_val) {
        return PROP(obj, prop, default_val, _.isFunction);
    }
    function PROP_OBJECT(obj, prop, default_val) {
        return PROP(obj, prop, default_val, _.isObject);
    }

    /**
     * The base class for widgets
     *
     * @returns {undefined}
     */
    function Widget() {
    }

    Class.define_class(Widget, null, {}, {

        /**
         * Display the widget ta the specified location
         * 
         * @param String or JQuery Object - a container where to render the widget
         */
        display: function(container) {
            switch (typeof container) {
                case 'string': this.container = $('#'+container); break;
                case 'object': this.container = $(container); break;
                default:
                    throw new WidgetError('Widget: the container parameter is mission or it has wrong type');
            }
            this.render();
        },

        /**
         * Render the widget. This method MUST be implemented by a subclasse
         */
        render: function() {
            throw new WidgetError('Widget: no rendering provided by the derived class');
        }

    });

    return {
        WidgetError:   WidgetError,
        ASSERT:        ASSERT,
        PROP:          PROP,
        PROP_STRING:   PROP_STRING,
        PROP_NUMBER:   PROP_NUMBER,
        PROP_BOOL:     PROP_BOOL,
        PROP_FUNCTION: PROP_FUNCTION,
        PROP_OBJECT:   PROP_OBJECT,
        Widget:        Widget
    };
});
