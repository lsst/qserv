define([
    'modules/sql-formatter.min'],

function(sqlFormatter) {
    class Common {
        static RestAPIVersion = 20;
        static query2text(query, expanded) {
            if (expanded) {
                return sqlFormatter.format(query, Common._sqlFormatterConfig);
            } else if (query.length > Common._max_compact_length) {
                return query.substring(0, Common._max_compact_length) + "...";
            } else {
                return query;
            }
        }
        static _sqlFormatterConfig = {"language":"mysql", "uppercase:":true, "indent":"  "};
        static _max_compact_length = 120;
    }
    return Common;
});
