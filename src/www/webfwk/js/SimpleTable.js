/**
 * The SimpleTable widget
 * 
 * Objects can be created by instantiating the constructor:
 *
 *   var table = new SimpleTable.constructor(id, coldef, rows, options, config_handler)
 * 
 * The class is designed to simplify creating dynamic tables
 * at a location specified by HTML container 'id'. Table configuration
 * is expected in parameter 'coldef' which is a JSON object. The JSON
 * object is a dictionary of column definitions:
 * 
 *   [ <col-def-0>, <col-def-1> .. <col-def-N> ]
 * 
 * Each column definition is a dictionary which allows to define either
 * a simple column:
 * 
 *   { name: <col-name>, type: <type-descriptor> }
 * 
 * or a composite column:
 * 
 *   { name: <col-name>, coldef: [ <col-def-0>, <col-def-1> .. <col-def-M> ] }
 *
 * Definitions for the bottom level columns may also provide additional parameters
 * to override the defaults:
 * 
 * 1. Data elements type:
 * 
 *      type: <type-descriptor>
 *
 *    Where, type descriptior can be either one of the predefined types:
 *
 *      SimpleTable.Types.Number_URL
 *      SimpleTable.Types.Number
 *      SimpleTable.Types.Text      <- the default type if none is used
 *      SimpleTable.Types.Text_URL
 *
 *    Where type Number_URL corresponds to the following data objects:
 *
 *      var value {
 *        number: 123,            // some number
 *        url:  "https://..."     // the corresponding URL
 *        ..
 *      };
 *
 *    Where the last type (Text_URL) corresponds to the following data objects:
 *
 *      var value {
 *        text: "123",           // some text
 *        url:  "https://..."    // the corresponding URL
 *        ..
 *      };
 *
 *    It's also possible to pass the type descriptor as a user defined dictionary
 *    of optional functions describing how to interpret data elements for
 *    a custom type and (optionally) what to do after sorting.
 *    For example, of a data element is an object:
 *    
 *      var value = {
 *        value: 123,
 *        url: 'https://....'
 *      };
 *
 *    then the type descriptor should look like:
 *
 *      type: {
 *        to_string:      function(a)   { return '<a href="'+a.url+'"; target="_blank";'>'+a.value+'</a>'; },
 *        compare_values: function(a,b) { return a.value - b.value; },
 *      }
 *
 *    Note, that all above mentioned members of the type descriptor
 *    are optional. If 'to_string' is not provided then the default method 'toString()'
 *    will be assumed. If 'compare_values' is not provided then value object will be
 *    used, which may result in unpredictable sort order of rows.
 *
 *    And finaly, there is an alternative method of creating a custom
 *    cell class. This would provide maximum flexibility when defining
 *    customazable and tunable column types. In theory one can imagine
 *    customising column types on the fly by communicating with those
 *    custom objects and triggering table redisplay.
 *
 *      function MyCellType() { SimpleTable.CellType.call(this); }
 *      define_class(MyCellType, SimpleTable.CellType, {}, {
 *        to_string     : function(a)   { return '<button class="my_button" name="'+a+'">'+a+'</button>'; },
 *        compare_values: function(a,b) { return this.compare_strings(a,b); },
 *        after_sort    : function()    { $('.my_button').button(); }}
 *      );
 *    The last parameter is optional. It will be triggered each time rows
 *    have been sorted. It allows to dynamically customize cells of
 *    thsi type.
 *
 *    Finally, instantiate an object and pass it as the column parameter:
 *
 *      type: new MyCellType()
 *    
 * 2. Sorting flag:
 *
 *      sorted: {true|false}
 *    
 *    Where the default is 'true'.
 *
 * 3. Hiding columns
 * 
 *      hideable: {true|false}
 *
 *    Where the default is 'false'.
 *
 * 4. Cell content alignment
 * 
 *      align: {left|right|center}
 *
 *    Where the default is 'left'
 *
 * 5. Extra styles for cells. For example:
 * 
 *      style: ' white-space: nowrap;'
 *
 * The 'rows' parameter may contain static row data for table cells
 * in case if this information is available at a time when the table
 * object is built. Otherwise use dynamic loading.
 *
 * Options are specified via the 'options' object (dictionary). The following
 * options are supported in this implementation of teh class:
 *
 *   'text_when_empty'      - HTML text to show when no row data are loaded
 *   'default_sort_column'  - an optional column number by which to sort (default: 0)
 *   'default_sort_forward' - the direction of the sort (default: true)
 *   'row_select_action'    - a function which will be fired when a row is selected.
 *                            the only parameter of the function will be an array
 *                            representing the corresponding row. The rows will be the same
 *                            which are passed into the table either at a time when the table
 *                            is created or dynamically loaded with the row data.
 *   
 * The 'config_handler' parameter (if provided) will be used to maintain a persistent
 * state of table configuration on teh server side.
 */

define([
    'webfwk/CSSLoader',
    'webfwk/Class',
    'webfwk/Widget'],

function(CSSLoader,
         Class,
         Widget) {

    CSSLoader.load('webfwk/css/SimpleTable.css');

    // Extended exception class for the table

    function TableError(message) {
        this.message = message;
    }
    TableError.prototype = new Error();

    // Cell types

    function CellType() {}
    Class.define_class(CellType, null, {}, {
        to_string:       function(a)   { return ''+a; },
        compare_numbers: function(a,b) { return a - b; },
        compare_strings: function(a,b) {
            var a_ = ''+a;
            var b_ = ''+b;
            return (a_ < b_) ? -1 : ((b_ < a_) ? 1 : 0); },
        compare_values: function(a,b) { return this.compare_strings(a,b); },
        after_sort    : function() {},
        select_action : function(a) {alert(a);}}
    );

    function CellType_Number() { CellType.call(this); }
    Class.define_class(CellType_Number, CellType, {}, {
        compare_values: function(a,b) { return this.compare_numbers(a,b); }}
    );

    function CellType_NumberURL() { CellType.call(this); }
    Class.define_class(CellType_NumberURL, CellType, {}, {
        compare_values: function(a,b) { return this.compare_numbers(a.number,b.number); },
        to_string     : function(a)   { return '<a class="table_link" href="'+a.url+'"; target="_blank";>'+a.number+'</a>'; }}
    );

    function CellType_NumberHTML() { CellType.call(this); }
    Class.define_class(CellType_NumberHTML, CellType, {}, {
        compare_values: function(a,b) { return this.compare_numbers(a.number,b.number); },
        to_string     : function(a)   { return a.html; }}
    );

    function CellType_Text() { CellType.call(this); }
    Class.define_class(CellType_Text, CellType, {}, {});

    function CellType_TextURL() { CellType.call(this); }
    Class.define_class(CellType_TextURL, CellType, {}, {
        to_string     : function(a)   { return '<a class="table_link" href="'+a.url+'"; target="_blank";>'+a.text+'</a>'; },
        compare_values: function(a,b) { return this.compare_strings(a.text,b.text); }}
    );
    var Types = {
        Number:      new CellType_Number(),
        Number_URL:  new CellType_NumberURL(),
        Number_HTML: new CellType_NumberHTML(),
        Text:        new CellType_Text(),
        Text_URL:    new CellType_TextURL()
    };
    var Status = {
        Empty  : '&lt;&nbsp;'+'empty'+'&nbsp;&gt;',
        Loading: '&nbsp;'+'loading...'+'&nbsp;',
        error  : function(msg) { return '<span style="color:red;">&lt;&nbsp;'+msg+'&nbsp;&gt;</span>'; }
    };

    // Helper functions for HTML generation

    function Attributes_HTML(attr) {
        var html = '';
        if (attr) {
            if (attr.id)       html += ' id="'+attr.id+'"';
            if (attr.classes)  html += ' class="'+attr.classes+'"';
            if (attr.name)     html += ' name="'+attr.name+'"';
            if (attr.value)    html += ' value="'+attr.value+'"';
            if (attr['size'])  html += ' size="'+attr['size']+'"';
            if (attr.disabled) html += ' disabled="disabled"';
            if (attr.checked)  html += ' checked="checked"';
            if (attr.onclick)  html += ' onclick="'+attr.onclick+'"';
            if (attr.title)    html += ' title="'+attr.title+'"';
            if (attr.extra)    html += ' '+attr.extra;
        }
        return html;
    }
    function TextInput_HTML(attr) {
        var html = '<input type="text"'+Attributes_HTML(attr)+'/>';
        return html;
    }
    function TextArea_HTML(attr,rows,cols) {
        var html = '<textarea rows="'+rows+'" cols="'+cols+'" '+Attributes_HTML(attr)+'/></textarea>';
        return html;
    }
    function Checkbox_HTML(attr) {
        var html = '<input type="checkbox"'+Attributes_HTML(attr)+'/>';
        return html;
    }
    function Button_HTML(name,attr) {
        attr.classes = 'btn ' + (attr.classes === undefined ? '' : attr.classes);
        var html = '<button type="button" '+Attributes_HTML(attr)+'>'+name+'</button>';
        return html;
    }
    function Select_HTML(options, selected, attr) {
        var html = '<select '+Attributes_HTML(attr)+'>';
        for (var i in options) {
            var opt = options[i];
            var selected_opt = opt == selected ? ' selected="selected" ' : '';
            html += '<option name="'+opt+'" '+selected_opt+'>'+opt+'</option>';
        }
        html += '</select>';
        return html;
    }
    var html = {
        TextInput: TextInput_HTML,
        TextArea:  TextArea_HTML,
        Checkbox:  Checkbox_HTML,
        Button:    Button_HTML,
        Select:    Select_HTML
    };

    /**
     * Table constructor
     *
     * @param {string or JQuery object} container
     * @param {object} coldef
     * @param {array} rows
     * @param {object} options
     * @param {function} config_handler
     * @returns {function}
     */
    function consructor(container,
                        coldef,
                        rows,
                        options,
                        config_handler) {

        var that = this;

        /* --------------------
         *   Static functions
         * --------------------
         */

        // Sorting functions

        function sort_func(type,column) {
            this.compare = function(a,b) {
                return type.compare_values(a[column], b[column]);
            }
        }
        function sort_func4cells(type) {
            this.compare = function(a,b) {
                return type.compare_values(a, b);
            }
        }
        function sort_sign_classes_if(condition,forward) {
            return condition ?
                ['ui-icon', (forward ? 'ui-icon-triangle-1-s' : 'ui-icon-triangle-1-n')] :
                ['ui-icon', 'ui-icon-triangle-2-n-s'];
        }

        /* ----------------------
         *   Instance functions
         * ----------------------
         */

        this.get_container = function() {
            return this.container;
        };
        this.cols = function() {
            return this.header.size.cols;
        };
        this.num_rows = function() {
            if (this.rows) {
                var num = 0;
                for (var i in this.rows) num++;
                return num;
            }
            return 0;
        };
        this.empty = function() {
            return this.num_rows() ? false : true;
        };
        this.selected_cell = function() {
            var cell = {
                row: this.selected_row,
                col: this.selected_col,
                obj: this.selected_object()
            };
            return cell;
        };
        this.selected_object = function() {
            return this.selected_row ? this.selected_row[this.selected_col] : null;
        };
        this._sort_enabled = true;
        this.enable_sort = function(yes_or_no) { this._sort_enabled = yes_or_no ? true : false; };
        this.sort_rows = function() {
            if (!this._sort_enabled) return;
            var column = this.sorted.column;
            if (!this.header.sorted[column]) return;
            var bound_sort_func = new sort_func(this.header.types[column], column);
            this.rows.sort(bound_sort_func.compare);
            if (!this.sorted.forward) this.rows.reverse();
        };
        this.load = function(rows, keep_selection) {

            // Preserve the state to be restored if 'keep_selection' requested

            var prev_selected_cell = this.selected_cell();
            var prev_empty         = this.empty();

            // Load/update the payload

            this.rows = rows ? rows : [];
            this.selected_row = rows ? rows[0] : null;

            this.display();

            // Try restoring the previous selection if possible

            if (keep_selection && !prev_empty && (prev_selected_cell.obj !== null))
                this.select(prev_selected_cell.col, prev_selected_cell.obj);
        };
        this.select = function(col, obj) {
            if ((col < 0) || (col >= this.cols()) || (col != this.selected_col)) return;
            var bound_sort_func4cells = new sort_func4cells(this.header.types[col]);
            for (var i in this.rows) {
                var row = this.rows[i];
                if (!bound_sort_func4cells.compare(row[col], obj)) {
                    this.selected_row = row;
                    this.display();
                    return;
                }
            }
        };
        this.header_info = function() {

            /**
             * Return an array of entries, each representing a bottom column of
             * the header:
             * 
             *   [ { number:   0,
             *       name:     'First',
             *       hideable: true,
             *       hidden:   false
             *     },
             *     ..
             *   ]
             */
            var info = [];
            for (var i in this.coldef) {
                var col = this.coldef[i];
                if (col.number !== undefined) {
                    var col_idx = col.number;
                    info.push ({
                        number:   col_idx,
                        name:     col.name,
                        hideable: this.header.hideable[col_idx],
                        hidden:   this.header.hidden  [col_idx]
                    });
                }
            }
            return info;
        };

        this.display = function(commands, arg1) {

            var that = this;

            /**
             * Process optional commands wich can be:
             * 
             * 'show_all'
             * 'hide_all'
             * 'hide' <col_num>
             * 
             */
            if (typeof(commands) === 'string') {
                if (commands === 'show_all') {
                    for (var col_idx in this.header.hidden) {
                        if (this.header.hideable[col_idx]) {
                            this.header.hidden[col_idx] = false;
                        }
                    }
                } else if (commands === 'hide_all') {
                    for (var col_idx in this.header.hidden) {
                        if (this.header.hideable[col_idx]) {
                            this.header.hidden[col_idx] = true;
                        }
                    }
                } else if (commands === 'show') {
                    if (typeof(arg1) === 'number') {
                        var col_idx = arg1;
                        if ((this.header.hideable[col_idx] !== undefined) && this.header.hideable[col_idx]) {
                            this.header.hidden[col_idx] = false;
                        }
                    }
                } else if (commands === 'hide') {
                    if (typeof(arg1) === 'number') {
                        var col_idx = arg1;
                        if ((this.header.hideable[col_idx] !== undefined) && this.header.hideable[col_idx]) {
                            this.header.hidden[col_idx] = true;
                        }
                    }
                }
                this.save_state();
            }

            /**
             * Render the table within a container provided as a parameter
             * of the table object. Each header cell is located at a level
             * which varies from 0 up to the total number of the full header's
             * rows minus 1.
             * 
             * NOTE: that because multi-level rows in HTML tables are produced
             * by walking an upper layer first and gradually procinging to lower-level
             * rows then we only drow header cells at the requested level.
             */

            var html = '<table class="simple-table table table-sm table-hover table-bordered"">' +
                       (this.options.caption !== undefined ? '<caption>' + this.options.caption + '</caption>' : '') +
                       '  <thead class="thead-light">';

            // Draw header

            for (var level2drows=0; level2drows < this.header.size.rows; ++level2drows) {
                html += '<tr>';
                for (var i in this.coldef) {
                    var col = this.coldef[i];
                    html += this.display_header(0,level2drows,col);
                }
                html += '</tr>';
            }
            html += '  </thead>' +
                    '<tbody>';

            // Draw rows (if available)

            if (this.rows.length) {
                this.sort_rows();
                for (var i in this.rows) {
                    html += '<tr '+(this.row_select_action ? 'class="table_row_selectable" id="'+i+'" ' : '')+'>';
                    var row = this.rows[i];
                    for (var j=0; j < row.length; ++j) {
                        var classes = '';
                        var selector = '';
                        if (this.header.selectable[j]) {
                            classes += ' table_cell_selectable';
                            if (row == this.selected_row) {
                                classes += ' table_cell_selectable_selected';
                            }
                            selector = ' id="'+j+' '+i+'"';
                        }
                        var styles = '';
                        if (this.header.style[j] != '') styles='style="'+this.header.style[j]+'"';
                        html += '<td class="'+classes+'" '+selector+' align="'+this.header.align[j]+'" valign="top" '+styles+' >';
                        if (this.header.hidden[j]) html += '&nbsp;';
                        else                      html += this.header.types[j].to_string(row[j]);
                        html += '</td>';
                    }
                    html += '</tr>';
                }
            } else {
                if (this.text_when_empty)
                    html +=
                        '<tr>'+
                        '<td colspan='+this.cols()+' rowspan=1 valign="top" >'+this.text_when_empty+'</td>'+
                        '</tr>';
            }
            html += '</tbody></table>';

            this.container.html(html);
            this.container.find('.table_row_sorter').click(function() {
                var column = parseInt($(this).find('.table_sort_sign').attr('name'));
                that.sorted.forward = !that.sorted.forward;
                that.sorted.column  = column;
                that.sort_rows();
                that.display();
                that.save_state();
            });
            for (var i in this.header.types) {
                this.header.types[i].after_sort(); 
            }
            this.container.find('.table_cell_selectable').click(function() {
                var addr = this.id.split(' ');
                var col_idx = addr[0];
                var row_idx = addr[1];
                that.selected_row  = that.rows[row_idx];
                that.header.types[col_idx].select_action(that.rows[row_idx][col_idx]);
                that.container.find('.table_cell_selectable_selected').removeClass('table_cell_selectable_selected');
                $(this).addClass('table_cell_selectable_selected');
            });
            this.container.find('.table_column_hider').find('input[type="checkbox"]').change(function() {
                var col_idx = parseInt(this.name);
                that.header.hidden[col_idx] = !this.checked;
                that.display();
                that.save_state();
            });
            this.container.find('.table_row_selectable').click(function() {
                var i = parseInt($(this).attr('id'));
                var row = that.rows[i];
                that.row_select_action(row);
            });

            if (this.common_after_sort) this.common_after_sort();
        };

        this.erase = function(text_when_empty) {
            if (text_when_empty) this.text_when_empty = text_when_empty;
            this.load([]);
        };

        this.display_header = function(level,level2drows,col) {
            var html = '';
            var rowspan = this.header.size.rows - level;
            var colspan = 1;
            if (col.coldef) {
                var child = this.header_size(col.coldef);
                rowspan -= child.rows;  // minus rows for children
                colspan = child.cols;   // columns for children
            }

            // Drawing is only done if we're at the right level

            if (level == level2drows) {

                var align = this.header.align[col.number] ? 'style="text-align:' + this.header.align[col.number] + '"' : '';
                var classes = '';
                var sort_sign = '';
                if (rowspan + level == this.header.size.rows) {
                    
                    // Bottomost header can be either hideable or sortable.

                    if (this.header.hideable[col.number]) {
                        classes += ' table_column_hider';
                    } else {
                        if (this.header.sorted[col.number]) {
                            classes += ' table_active_hdr table_row_sorter'+(this.sorted.column == col.number ? ' table_active_hdr_selected' : '');
                            var sort_sign_classes = sort_sign_classes_if(this.sorted.column == col.number, this.sorted.forward);
                            sort_sign = '<span class="table_sort_sign';
                            for (var i in sort_sign_classes) sort_sign += ' '+sort_sign_classes[i];
                            sort_sign += '" name="'+col.number+'">'+'</span>';
                        }
                    }
                }

                var col_html = col.name+'&nbsp;'+sort_sign;
                html += '<th class="'+classes+'" rowspan='+rowspan+' colspan='+colspan+' '+align+' >';
                if ((rowspan + level == this.header.size.rows) && this.header.hideable[col.number]) {
                    if (this.header.hidden[col.number]) {
                        html += '<input type="checkbox" name="'+col.number+'" title="check to expand the column: '+col.name+'"/>';
                    } else {
                        html += '<input type="checkbox" name="'+col.number+'" checked="checked" title="uncheck to hide the column"/>&nbsp;'+col_html;
                    }
                } else {
                    html += col_html;
                }                
                html += '</th>';
            }

            // And to optimize things we stop walking the header when the level drops
            // below the level where we're supposed to drow things.

            if ((level2drows > level) && col.coldef) {
                for (var i in col.coldef) {
                    var child_col = col.coldef[i];
                    html += this.display_header(level+1,level2drows,child_col);
                }
            }
            return html;
        };

        this.header_size = function(coldef) {

            /**
             * Traverse colum definition and return the maximum limits
             * for the table header, including:
             * 
             *   rows: the number of rows needed to represent the full header
             *   cols: the total number of low-level columns for the data
             */

            var rows2return = 0;
            var cols2return = 0;
            for (var i in coldef) {
                var col  = coldef[i];
                var rows = 1;
                var cols = 1;
                if (col.coldef) {
                    var child = this.header_size(col.coldef);
                    rows += child.rows;
                    cols  = child.cols;
                }
                if (rows > rows2return) rows2return = rows;
                cols2return += cols;
            }
            return {rows: rows2return, cols: cols2return};
        };

        /**
         * 
         * @param {type} coldef
         * @param {type} types
         * @param {type} sorted
         * @param {type} hideable
         * @param {type} hidden
         * @param {type} selectable
         * @param {type} align
         * @param {type} style
         * @param {type} next_column_number
         * @returns {SimpleTableL#143.consructor.column_types.SimpleTableAnonym$5}
         */
        this.column_types = function(
                coldef,
                types,
                sorted,
                hideable,
                hidden,
                selectable,
                align,
                style,
                next_column_number) {

            /**
             * Traverse colum definition and return types for the bottom-most
             * header cells.
             */
            for (var i in coldef) {
                var col = coldef[i];
                if (col.coldef) {
                    var child = this.column_types(col.coldef,
                                                  types,
                                                  sorted,
                                                  hideable,
                                                  hidden,
                                                  selectable,
                                                  align,
                                                  style,
                                                  next_column_number);

                    types              = child.types;
                    sorted             = child.sorted;
                    hideable           = child.hideable;
                    hidden             = child.hidden;
                    selectable         = child.selectable;
                    align              = child.align;
                    style              = child.style;
                    next_column_number = child.next_column_number;
                } else {
                    if (col.type) {
                        if ($.isPlainObject(col.type)) {
                            var type = function() { CellType.call(this); };
                            Class.define_class(type, CellType, {}, col.type);
                            types.push(new type());
                        } else {
                            types.push(col.type);
                        }
                    } else {
                        types.push(Types.Text);
                    }
                    sorted.push    (col.sorted     !== undefined ? col.sorted     : true);
                    hideable.push  (col.hideable   !== undefined ? col.hideable   : false);
                    hidden.push    (false);
                    selectable.push(col.selectable !== undefined ? col.selectable : false);
                    align.push     (col.align      !== undefined ? col.align      : 'left');
                    style.push     (col.style      !== undefined ? col.style      : '');
                    col.number = next_column_number++;
                }
            }
            return {
                types:              types,
                sorted:             sorted,
                hideable:           hideable,
                hidden:             hidden,
                selectable:         selectable,
                align:              align,
                style:              style,
                next_column_number: next_column_number
            };
        };

        this.load_state = function (persistent_state) {
            this.header.hidden = persistent_state.hidden;
            this.sorted        = persistent_state.sorted;
            this.display();
        };
        this.save_state = function () {
            if (this.config_handler) {
                var persistent_state = {
                    hidden: this.header.hidden,
                    sorted: this.sorted
                };
                this.config_handler.save(persistent_state);
            }
        };

        /* -----------------------------------------------
         *   Process and store parameters of an instance
         * -----------------------------------------------
         */
        this.config_handler = config_handler;

        // container address where to render the table

        switch (typeof container) { 
            case 'string' : this.container = $('#'+container); break;
            case 'object' : this.container = $(container); break;
            default :
                throw new TableError('SimpleTable: wrong type of the table container parameter');
        }
        this.coldef = jQuery.extend(true,[],coldef);    // columns definition: make a deep local copy

        // Optional parameters

        this.rows            = rows ? rows : [];
        this.options         = options ? options : {};
        this.text_when_empty = this.options.text_when_empty !== undefined ? this.options.text_when_empty : Status.Empty;


        // Sort configuration

        this.sorted = {
            column:  this.options.default_sort_column  !== undefined ? this.options.default_sort_column  : 0,   // the number of a column by which rows are sorted
            forward: this.options.default_sort_forward !== undefined ? this.options.default_sort_forward : true // sort direction
        };
        this.common_after_sort = this.options.common_after_sort !== undefined ? this.options.common_after_sort : null;
        this.selected_col = this.options.selected_col !== undefined ? this.options.selected_col : 0;
        this.selected_row = rows ? rows[0] : null;

        this.header = {
            size: {cols: 0, rows: 0},
            types: [],
            sorted: [],
            hideable: [],
            hidden: [],
            selectable: [],
            align: [],
            style: []
        };
        this.header.size  = this.header_size(this.coldef);

        var bottom_columns = this.column_types(
            this.coldef,
            this.header.types,
            this.header.sorted,
            this.header.hideable,
            this.header.hidden,
            this.header.selectable,
            this.header.align,
            this.header.style,
            0
        );
        this.header.types      = bottom_columns.types;
        this.header.sorted     = bottom_columns.sorted;
        this.header.hideable   = bottom_columns.hideable;
        this.header.hidden     = bottom_columns.hidden;
        this.header.selectable = bottom_columns.selectable;
        this.header.align      = bottom_columns.align;
        this.header.style      = bottom_columns.style;

        if (this.options.row_select_action) {
            if (typeof this.options.row_select_action !== 'function')
                throw new TableError("SimpleTable: wrong type of the 'row_select_action option', must be a function");
            this.row_select_action = this.options.row_select_action;
        }

        // Override table configuration parameters from the persistent store.
        // Note that if the store already has a cached value then we're going to use
        // the one immediatelly. Otherwise the delayed loader will kick in
        // when a transcation to load from an external source will finish.
        // In case if there is not such parameter in teh external source we will
        // push our current state to that store for future uses.

        if (this.config_handler) {
            var persistent_state = this.config_handler.load(
                function(persistent_state) { that.load_state(persistent_state); },
                function()                 { that.save_state(); }
            );
            if (persistent_state) {
                this.header.hidden = persistent_state.hidden;
                this.sorted        = persistent_state.sorted;
            }
        }
    }
    Class.define_class(consructor, null, {},{});

    return {
        constructor: consructor,
        CellType:    CellType,
        Types:       Types,
        Status:      Status,
        html:        html
    };
});
