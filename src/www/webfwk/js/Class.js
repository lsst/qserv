(function () {

    /**
     * Utility function for creating base or derived classes.
     * 
     *   function Base(value) {
     *       this.value = value;
     *       Base.num_instances++;
     *   }
     *   define_class( Base, null, {
     *       num_instances: 0
     *   },{
     *       set_value: function(value) { this.value = value; },
     *       get_value: function()      { return this.value;  }}
     *   );
     *   var obj1 = new Base(123);
     *   alert(Base.num_instances);
     *
     *
     *   function Derived(value) {
     *       Base.call(this,value);
     *   }
     *   define_class( Derived, Base, {},{
     *       get_value: function() { return 'Derived: '+this.value;  }}
     *   );
     *   var obj2 = new Derived(1);
     *   alert(Base.num_instances);
     */
    function define_class(constructor, base, statics, methods ) {

        if (base) {
            if (Object.create)
                constructor.prototype = Object.create(base.prototype);
            else { 
                function f() {};
                f.prototype = base.prototype;
                constructor.prototype = new f();
            }
        }
        constructor.prototype.constructor = constructor;

        if (statics)
            for (var s in statics)
                constructor[s] = statics[s];

        if (methods)
            for (var m in methods)
                constructor.prototype[m] = methods[m];

        return constructor;
    }

    /*
     * To make this an AMD module (for loading with RequireJS, etc.)
     */
    if (typeof define === "function" && define.amd)
        define (
            [],
            function() {
                return {
                    define_class: define_class
                }
            }
        );

}).call(this);
