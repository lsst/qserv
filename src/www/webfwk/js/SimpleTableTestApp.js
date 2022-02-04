define([
    'webfwk/Class',
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkApplication',
    'webfwk/SimpleTable',
    'underscore'],

function(Class,
         CSSLoader,
         Fwk,
         FwkApplication,
         SimpleTable) {

    CSSLoader.load('webfwk/css/SimpleTableTestApp.css');

    class SimpleTableTestApp extends FwkApplication {

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
            this._init();
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
                this._init();
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

            let html = `
<div id="fwk-uitests-simpletable">
  <div class="table-cont" id="table1"></div>
  <div class="table-cont" id="table2"></div>
  <div class="table-cont" id="table3"></div>
  <div class="table-cont-last"></div>
  <div class="table-cont">
    <div id="table4"></div>
    <button type="button" class="btn btn-success btn-sm" id="table4-load">Load</button>
    <button type="button" class="btn btn-danger  btn-sm" id="table4-erase">Erase</button>
  </div>
  <div class="table-cont">
    <div id="table5">Loading...</div>
    <button type="button" class="btn btn-success btn-sm" id="table5-load">Load</button>
  </div>
  <div class="table-cont" id="table6"></div>
  <div class="table-cont" id="table7"></div>
  <div class="table-cont-last"></div>
</div>`;
            this.fwk_app_container.html(html);

            this._init_table1();
            this._init_table2();
            this._init_table3();
            this._init_table4();
            this._init_table5();
            this._init_table6();
            this._init_table7();
        }

        _cont() {
            if (this._cont_obj === undefined) {
                this._cont_obj = this.fwk_app_container.children('#fwk-uitests-simpletable');
            }
            return this._cont_obj;
        }
        _init_table1() {
            if (this._table1_obj === undefined) {
                this._table1_obj = new SimpleTable.constructor(
                    this._cont().find('#table1'),

                    [{name: '1'},
                     {name: '2'},
                     {name: '3', sorted: false}],

                    [['1(1)','2(2)','3(2)'],
                     ['2(1)','3(2)','4(2)'],
                     ['3(1)','4(2)','1(2)'],
                     ['4(1)','1(2)','2(2)']],

                    // options
                    {caption: "Simple Header"}
                );
                this._table1_obj.display();
            }
        }
 
        _init_table2() {
            if (this._table2_obj === undefined) {
                this._table2_obj = new SimpleTable.constructor(
                    this._cont().find('#table2'),

                    [{name: 'id'},
                     {name: '2', coldef: [
                        {name: '2.1'},
                        {name: '2.2'}]},
                     {name: '3'},
                     {name: '4'}],

                    [['1', '1(2.1)', '1(2.2)', '1(3)', '1(4)'],
                     ['2', '2(2.1)', '2(2.2)', '2(3)', '2(4)'],
                     ['3', '3(2.1)', '3(2.2)', '3(3)', '3(4)'],
                     ['4', '4(2.1)', '4(2.2)', '4(3)', '4(4)']],

                    // options
                    {caption: "Header 2"}
                );
                this._table2_obj.display();
            }
        }

        _init_table3() {
            if (this._table3_obj === undefined) {
                this._table3_obj = new SimpleTable.constructor(
                    this._cont().find('#table3'),

                    [{name: 'id'},
                     {name: '2', coldef: [
                        {name: '2.1', coldef: [
                            {name: '2.1.1'},
                            {name: '2.1.2'}]},
                        {name: '2.2'}]},
                      {name: '3'},
                      {name: '4'}],

                    [['1', '1(2.1.1)', '1(2.1.2)', '1(2.2)', '1(3)', '1(4)'],
                     ['2', '2(2.1.1)', '2(2.1.2)', '2(2.2)', '2(3)', '2(4)'],
                     ['3', '3(2.1.1)', '3(2.1.2)', '3(2.2)', '3(3)', '3(4)'],
                     ['4', '4(2.1.1)', '4(2.1.2)', '4(2.2)', '4(3)', '4(4)']],

                    // options
                    {caption: "Three Layer Header"}
                );
                this._table3_obj.display();
            }
        }

        _init_table4() {
            if (this._table4_obj === undefined) {

                function MyCellType() {
                    SimpleTable.CellType.call(this);
                }
                Class.define_class(MyCellType, SimpleTable.CellType, {}, {
                   to_string:      function(a)   { return '<b>'+a+'</b>'; } ,
                   compare_values: function(a,b) { return this.compare_strings(a,b); }}
                ) ;
                this._table4_obj = new SimpleTable.constructor(
                    this._cont().find('#table4'),

                    [{name: 'Text_URL',   type: SimpleTable.Types.Text_URL},
                     {name: 'Number_URL', type: SimpleTable.Types.Number_URL} ,
                     {name: 'MyCellType', type: new MyCellType},
                     {name: 'Customized', type: {to_string:      function(a)   { return SimpleTable.html.Button(a.data, {name: a.data, classes: 'btn-info btn-sm my_button'}); },
                                                 compare_values: function(a,b) { return a.data - b.data ; } ,
                                                 after_sort:     function()    { $('.my_button').button().click(function () { alert(this.name); }); }}}],

                    // No data rows yet
                    null,

                    // options
                    {   caption:         "In-memory data, active cells",
                        text_when_empty: SimpleTable.Status.Empty
                    }
                );
                this._table4_obj.display();

                let data4 = [
                    [{text: 'A',         url: 'https://www.slac.stanford.edu'}, {number: 123, url: 'https://www.slac.stanford.edu'}, '3(2)', {data:  3}],
                    [{text: 'a',         url: 'https://www.slac.stanford.edu'}, {number: -99, url: 'https://www.slac.stanford.edu'}, '4(2)', {data: 11}],
                    [{text: 'xYz',       url: 'https://www.slac.stanford.edu'}, {number:   3, url: 'https://www.slac.stanford.edu'}, '1(2)', {data: 12}],
                    [{text: 'let it be', url: 'https://www.slac.stanford.edu'}, {number:   0, url: 'https://www.slac.stanford.edu'}, '2(2)', {data:  1}]
                ] ;
                this._cont().find('#table4-load').button().click(() => {
                    this._table4_obj.load(data4);
                });
                this._cont().find('#table4-erase').button().click(() => {
                    this._table4_obj.erase();
                });
            }
        }

        _init_table5() {
            if (this._table5_obj === undefined) {
                this._table5_obj = new SimpleTable.constructor(
                    this._cont().find('#table5'),

                    [{name: 'Number', type: SimpleTable.Types.Number},
                     {name: 'Text'}],

                    // No data rows yet
                    null,

                    // options
                    {caption: "Remote data"}
                );
                this._table5_obj.display();

                this._cont().find('#table5-load').button().click(() => {
                    this._table5_obj.erase(SimpleTable.Status.Loading);
                    $.ajax({
                        type: 'GET',
                        url:  'webfwk/ws/table_data.json' ,
                        success: (data) => {
                            this._table5_obj.load(JSON.parse(data));
                        },
                        error: () => {
                            this._table5_obj.erase(SimpleTable.Status.error('service is not available'));
                        },
                        dataType: 'html'
                    });
                });
            }
        }

        _init_table6() {
            if (this._table6_obj === undefined) {
                this._table6_obj = new SimpleTable.constructor(
                    this._cont().find('#table6'),

                    [{name: '1'} ,
                     {name: '2'} ,
                     {name: 'hideable', hideable: true},
                     {name: '4',                                       align: 'center'},
                     {name: '5',        hideable: true, sorted: false, align: 'right'}],

                    [['1(1)','2(2)','3(3)',     4,    5],
                     ['2(1)','3(2)','4(3)', 12554,  333],
                     ['3(1)','4(2)','1(3)',     1,   23],
                     ['4(1)','1(2)','2(3)',    21,    0],
                     ['7(1)','8(2)','9(3)',    56, 1999]],


                    // options
                    {caption: "Hideable columns"}
                );
                this._table6_obj.display();
            }
        }

        _init_table7() {
            if (this._table7_obj === undefined) {
                this._table7_obj = new SimpleTable.constructor(
                    this._cont().find('#table7'),

                    [{name: '1'},
                     {name: 'custom style', style:   'color: red; font-size: 125%'},
                     {name: 'last',         hideable: true, sorted: true, align: 'right'}],

                    [['1(1)',     4,    5],
                     ['2(1)', 12554,  333],
                     ['3(1)',     1,   23],
                     ['4(1)',    21,    0],
                     ['7(1)',    56, 1999]],


                    // options
                    {caption: "Custom cell styles"}
                );
                this._table7_obj.display();
            }
        }
    }
    return SimpleTableTestApp;
});

