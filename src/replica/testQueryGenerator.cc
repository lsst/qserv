/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <list>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/DatabaseMySQLGenerator.h"

// Boost unit test header
#define BOOST_TEST_MODULE QueryGenerator
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;
using namespace lsst::qserv::replica::database::mysql;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(QueryGeneratorTest) {
    LOGS_INFO("QueryGenerator test begins");

    QueryGenerator const g;

    TransactionId const transactionId = 12;
    vector<string> const workers0, workers2 = {"worker-1", "worker-2"};
    vector<string> const databases0, databases2 = {"dbA", "dbB"};
    string const subQueryText = g.select("worker") + g.from("workers");
    auto const subQuery = g.subQuery(subQueryText);

    string valuesPackedIncrementally = g.packVals(1, "abc");
    g.packVal(valuesPackedIncrementally, false);
    g.packVal(valuesPackedIncrementally, 1.234567f);

    string conditionsPackedIncrementally = g.packConds();
    g.packCond(conditionsPackedIncrementally, g.eq("col", 123));
    g.packCond(conditionsPackedIncrementally, "");
    g.packCond(conditionsPackedIncrementally, g.in("database", databases2));
    g.packCond(conditionsPackedIncrementally, string());
    g.packCond(conditionsPackedIncrementally, g.in("worker", workers2));

    // Column and key definitions for generating CREATE TABLE ... queries

    string const insertPacked =
            "INSERT INTO `Object` (`col1`,`col2`) VALUES (1,'abc') ON DUPLICATE KEY UPDATE "
            "`col1`=1,`col2`='abc'";

    list<SqlColDef> const columns = {
            {"id", "INT NOT NULL"}, {"col1", "VARCHAR(256) DEFAULT=''"}, {"col2", "DOUBLE"}};

    list<string> const keys = {g.packTableKey("PRIMARY KEY", "", "id"),
                               g.packTableKey("UNIQUE KEY", "composite", "col1", "col2")};

    string const createTableOne =
            "CREATE TABLE IF NOT EXISTS `one` ("
            ") ENGINE=InnoDB";

    string const createDbTableOne =
            "CREATE TABLE `db`.`one` ("
            ") ENGINE=InnoDB";

    string const createTableTwo =
            "CREATE TABLE `two` ("
            "`id` INT NOT NULL,`col1` VARCHAR(256) DEFAULT='',`col2` DOUBLE"
            ") ENGINE=MyISAM COMMENT='the comment'";

    string const createTableThree =
            "CREATE TABLE `three` ("
            "`id` INT NOT NULL,`col1` VARCHAR(256) DEFAULT='',`col2` DOUBLE,"
            "PRIMARY KEY (`id`),UNIQUE KEY `composite` (`col1`,`col2`)"
            ") ENGINE=InnoDB";

    string const createTableFour =
            "CREATE TABLE `four` ("
            "`id` INT NOT NULL,`col1` VARCHAR(256) DEFAULT='',`col2` DOUBLE,"
            "PRIMARY KEY (`id`),UNIQUE KEY `composite` (`col1`,`col2`)"
            ") ENGINE=MyISAM"
            " COMMENT='partitioned table'"
            " PARTITION BY LIST (`qserv_trans_id`)"
            " (PARTITION `p1` VALUES IN (1))";

    list<tuple<string, unsigned int, bool>> const indexKeys = {make_tuple("worker", 0, true),
                                                               make_tuple("status", 0, false)};
    list<tuple<string, unsigned int, bool>> const indexKeys2 = {make_tuple("worker", 16, true)};

    // Using current generator interface
    list<pair<string, string>> const tests = {

            // Identifiers
            {"`column`", g.id("column").str},
            {"`db`.`table`", g.id("db", "table").str},
            {"`table`.*", g.id("table", Sql::STAR).str},
            {"`p12`", g.partId(transactionId).str},
            {"DISTINCT `col`", g.distinctId("col").str},
            {"LAST_INSERT_ID()", Sql::LAST_INSERT_ID.str},
            {"COUNT(*)", Sql::COUNT_STAR.str},
            {"*", Sql::STAR.str},
            {"DATABASE()", Sql::DATABASE.str},
            {"NOW()", Sql::NOW.str},
            {"UNIX_TIMESTAMP(`time`)", Sql::UNIX_TIMESTAMP(g.id("time")).str},
            {"UNIX_TIMESTAMP(`time`)", g.UNIX_TIMESTAMP("time").str},
            {"UNIX_TIMESTAMP(`table`.`column`)", g.UNIX_TIMESTAMP(g.id("table", "column")).str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,NOW())",
             Sql::TIMESTAMPDIFF("SECOND", g.id("submitted"), Sql::NOW).str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,`completed`)",
             Sql::TIMESTAMPDIFF("SECOND", g.id("submitted"), g.id("completed")).str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,`completed`)",
             g.TIMESTAMPDIFF("SECOND", "submitted", "completed").str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,NOW())", g.TIMESTAMPDIFF("SECOND", "submitted", Sql::NOW).str},
            {"TIMESTAMPDIFF(SECOND,`table`.`submitted`,`table`.`completed`)",
             g.TIMESTAMPDIFF("SECOND", g.id("table", "submitted"), g.id("table", "completed")).str},

            // Values
            {"1", g.val(true).str},
            {"0", g.val(false).str},
            {"123", g.val(123).str},
            {"-123", g.val(-123).str},
            {"1.234567", g.val(1.234567f).str},
            {"'abc'", g.val("abc").str},
            {"'abc'", g.val(string("abc")).str},
            {"DO_NOT_PROCESS", g.val(DoNotProcess("DO_NOT_PROCESS")).str},
            {"SUM(`size`)", g.val(DoNotProcess("SUM(" + g.id("size").str + ")")).str},
            {"NULL", g.val(Sql::NULL_).str},
            {"LAST_INSERT_ID()", g.val(Sql::LAST_INSERT_ID).str},

            // Conditional injection of values
            {"'abc'", g.val(g.nullIfEmpty("abc")).str},
            {"NULL", g.val(g.nullIfEmpty("")).str},
            {"NULL", g.val(g.nullIfEmpty(string())).str},
            {"'a,b,c,d'", g.val(vector<string>({"a", "b", "c", "d"})).str},

            // Packing lists of values
            {"", g.packVals()},
            {"1", g.packVals(1)},
            {"''", g.packVals("")},
            {"''", g.packVals(string())},
            {"1,''", g.packVals(1, "")},
            {"1,0,123,-123,1.234567,'abc','abc',DO_NOT_PROCESS,NULL,LAST_INSERT_ID()",
             g.packVals(true, false, 123, -123, 1.234567f, "abc", string("abc"),
                        DoNotProcess("DO_NOT_PROCESS"), Sql::NULL_, Sql::LAST_INSERT_ID)},
            {"1,'abc',0,1.234567", valuesPackedIncrementally},

            {"(" + subQueryText + ")", subQuery.str},

            // Preparing complete IN or NOT IN clauses
            {"", g.in("worker", workers0)},
            {"`worker` IN ('worker-1','worker-2')", g.in("worker", workers2)},
            {"", g.notIn("worker", workers0)},
            {"`worker` NOT IN ('worker-1','worker-2')", g.notIn("worker", workers2)},

            // Preparing complete IN or NOT IN clauses with a subquery
            {"", g.inSubQuery("worker", "")},
            {"`worker` IN " + subQuery.str, g.inSubQuery("worker", subQuery)},
            {"`worker` IN (" + subQueryText + ")", g.inSubQuery("worker", subQueryText)},
            {"`config`.`worker` IN " + subQuery.str, g.inSubQuery(g.id("config", "worker"), subQuery)},
            {"", g.notInSubQuery("worker", "")},
            {"`worker` NOT IN " + subQuery.str, g.notInSubQuery("worker", subQuery)},

            // Packed conditions that's used in the WHERE clause
            {"", g.packConds()},
            {"`col`=123", g.packConds(g.eq("col", 123))},
            {"`col`=123 AND `database` IN ('dbA','dbB')",
             g.packConds(g.eq("col", 123), g.in("database", databases2))},
            {"", g.packConds(g.in("database", databases0))},
            {"`col`=123 AND `database` NOT IN ('dbA','dbB')",
             g.packConds(g.eq("col", 123), g.notIn("database", databases2))},
            {"`col`=123 AND `database` IN ('dbA','dbB') AND `worker` IN ('worker-1','worker-2')",
             conditionsPackedIncrementally},

            // Predicates to support searches using the FULL TEXT indexes
            {"MATCH(`query`) AGAINST('dp02' IN NATURAL LANGUAGE MODE)",
             g.matchAgainst("query", "dp02", "NATURAL LANGUAGE")},
            {"MATCH(`QInfo`.`query`) AGAINST('dp02' IN BOOLEAN MODE)",
             g.matchAgainst(g.id("QInfo", "query"), "dp02", "BOOLEAN")},

            // Preparing complete WHERE clause
            {"", g.where()},
            {" WHERE `col`=123", g.where(g.eq("col", 123))},
            {" WHERE `col`=123 AND `database` IN ('dbA','dbB')",
             g.where(g.eq("col", 123), g.in("database", databases2))},
            {"", g.where(g.in("database", databases0))},
            {" WHERE `col`=123 AND `database` NOT IN ('dbA','dbB')",
             g.where(g.eq("col", 123), g.notIn("database", databases2))},

            // Preparing complete WHERE clause with a sub-query
            {" WHERE `col`=123 AND `worker` NOT IN " + subQuery.str,
             g.where(g.eq("col", 123), g.notInSubQuery("worker", subQuery))},

            // Binary operators
            {"`col`=123", g.eq("col", 123)},
            {"`col`=1", g.eq("col", true)},
            {"`col`='abc'", g.eq("col", "abc")},
            {"`col`=NULL", g.eq("col", Sql::NULL_)},
            {"`col`!=123", g.neq("col", 123)},
            {"`col`<123", g.lt("col", 123)},
            {"`col`<=123", g.leq("col", 123)},
            {"`col`>123", g.gt("col", 123)},
            {"`col`>123", g.gt(g.id("col"), 123)},
            {"UNIX_TIMESTAMP(`time`)>1234567890", g.gt(g.UNIX_TIMESTAMP("time"), 1234567890)},
            {"NOW()>1234567890", g.gt(Sql::NOW, 1234567890)},
            {"`col`>=123", g.geq("col", 123)},
            {"`col` REGEXP '[0-9]+'", g.regexp("col", "[0-9]+")},
            {"`col` LIKE '%abc%'", g.like("col", "%abc%")},
            {"NOW()<=UNIX_TIMESTAMP(`time`)", g.op2(Sql::NOW, g.UNIX_TIMESTAMP("time"), "<=")},
            {"NOW()=`time`", g.op2(Sql::NOW, g.id("time"), "=")},

            // Packed pairs for using in INSERT ... VALUES()
            {"", g.packPairs()},
            {"`col1`='abc',`col2`='c',`col3`=123,`col4`=LAST_INSERT_ID()",
             g.packPairs(make_pair("col1", "abc"), make_pair("col2", string("c")), make_pair("col3", 123),
                         make_pair("col4", Sql::LAST_INSERT_ID))},

            // Preparing complete ORDER BY clause
            {"", g.orderBy()},
            {" ORDER BY `col1`", g.orderBy(make_pair("col1", ""))},
            {" ORDER BY `col1`", g.orderBy(make_pair(string("col1"), ""))},
            {" ORDER BY `col1`", g.orderBy(make_pair("col1", string()))},
            {" ORDER BY `col1`", g.orderBy(make_pair(string("col1"), string()))},
            {" ORDER BY `col1` DESC", g.orderBy(make_pair("col1", "DESC"))},
            {" ORDER BY `col1` ASC,`col2` DESC",
             g.orderBy(make_pair("col1", "ASC"), make_pair("col2", "DESC"))},
            {" ORDER BY `col1` ASC,`col2` DESC,`col3`",
             g.orderBy(make_pair("col1", "ASC"), make_pair("col2", "DESC"), make_pair("col3", ""))},

            // Pack collections of columns into strings
            {"", g.packIds()},
            {"`col1`", g.packIds("col1")},
            {"`col1`", g.packIds(string("col1"))},
            {"`col1`,`col2`", g.packIds("col1", "col2")},
            {"`col1`,`col2`,`col3`", g.packIds("col1", "col2", string("col3"))},
            {"LAST_INSERT_ID()", g.packIds(Sql::LAST_INSERT_ID)},
            {"COUNT(*)", g.packIds(Sql::COUNT_STAR)},
            {"*", g.packIds(Sql::STAR)},
            {"`category`,COUNT(*)", g.packIds("category", Sql::COUNT_STAR)},

            // Preparing complete GROUP BY clause
            {"", g.groupBy()},
            {" GROUP BY `col1`", g.groupBy("col1")},
            {" GROUP BY `col1`", g.groupBy(string("col1"))},
            {" GROUP BY `col1`,`col2`", g.groupBy("col1", "col2")},
            {" GROUP BY `col1`,`col2`,`col3`", g.groupBy("col1", "col2", "col3")},

            // Preparing complete LIMIT clause
            {"", g.limit(0)},
            {" LIMIT 123", g.limit(123)},
            {" LIMIT 123", g.limit(123, 0)},
            {" LIMIT 123 OFFSET 1", g.limit(123, 1)},

            // Complete INSERT queries
            {"INSERT INTO `Object` VALUES ()", g.insert("Object")},
            {"INSERT INTO `Object` VALUES (123456,'abc',1)", g.insert("Object", 123456, "abc", true)},
            {insertPacked, g.insertPacked("Object", g.packIds("col1", "col2"), g.packVals(1, "abc"),
                                          g.packPairs(make_pair("col1", 1), make_pair("col2", "abc")))},

            // Bulk insert of many rows
            {"INSERT INTO `Team` (`id`,`timestamp`,`name`)"
             " VALUES (NULL,NOW(),'John Smith'),(NULL,NOW(),'Vera Rubin'),(NULL,NOW(),'Igor Gaponenko')",
             g.insertPacked("Team", g.packIds("id", "timestamp", "name"),
                            {g.packVals(Sql::NULL_, Sql::NOW, "John Smith"),
                             g.packVals(Sql::NULL_, Sql::NOW, "Vera Rubin"),
                             g.packVals(Sql::NULL_, Sql::NOW, "Igor Gaponenko")})},

            // Complete UPDATE queries
            {"UPDATE `Object` SET `col1`='abc',`col2`=345",
             g.update("Object", make_pair("col1", "abc"), make_pair("col2", 345))},
            {"UPDATE `Object` SET `col1`='abc',`col2`=345 WHERE `id`=123",
             g.update("Object", make_pair("col1", "abc"), make_pair("col2", 345)) + g.where(g.eq("id", 123))},

            // Complete DELETE queries
            {"DELETE FROM `workers`", g.delete_("workers")},
            {"DELETE FROM `config`.`workers` WHERE `is_offline`=1 AND `worker` IN ('worker-1','worker-2')",
             g.delete_(g.id("config", "workers")) +
                     g.where(g.eq("is_offline", true), g.in("worker", workers2))},

            // Key generator to be used for generating CREATE TABLE queries
            {"PRIMARY KEY (`id`)", g.packTableKey("PRIMARY KEY", "", "id")},
            {"UNIQUE KEY `composite` (`col1`,`col2`)",
             g.packTableKey("UNIQUE KEY", "composite", "col1", "col2")},

            // CREATE TABLE ...
            {" PARTITION BY LIST (`qserv_trans_id`)", g.partitionByList("qserv_trans_id")},
            {" (PARTITION `p1` VALUES IN (1))", g.partition(1)},

            {createTableOne, g.createTable("one", true, list<SqlColDef>())},
            {createTableOne, g.createTable(g.id("one"), true, list<SqlColDef>())},
            {createDbTableOne, g.createTable("db", "one", false, list<SqlColDef>())},
            {createDbTableOne, g.createTable(g.id("db", "one"), false, list<SqlColDef>())},
            {createTableTwo, g.createTable("two", false, columns, list<string>(), "MyISAM", "the comment")},
            {createTableThree, g.createTable("three", false, columns, keys)},
            {createTableFour, g.createTable("four", false, columns, keys, "MyISAM", "partitioned table") +
                                      g.partitionByList("qserv_trans_id") + g.partition(1)},

            {"CREATE TABLE `dst` LIKE `src`", g.createTableLike("dst", "src")},
            {"CREATE TABLE IF NOT EXISTS `dst` LIKE `src`",
             g.createTableLike(g.id("dst"), g.id("src"), true)},

            // DROP TABLE [IF EXISTS] ...
            {"DROP TABLE `table`", g.dropTable("table")},
            {"DROP TABLE IF EXISTS `table`", g.dropTable("table", true)},

            // REPLACE INTO ...
            {"REPLACE INTO `table` VALUES (1,'abc')", g.replace("", "table", 1, "abc")},
            {"REPLACE INTO `db`.`table` VALUES (1,'abc',LAST_INSERT_ID())",
             g.replace("db", "table", 1, "abc", Sql::LAST_INSERT_ID)},

            // SELECT ...
            {"COUNT(*) AS `num`", g.as(Sql::COUNT_STAR, "num").str},
            {"0 AS `id`", g.as(DoNotProcess("0"), "id").str},
            {"`long_col_name` AS `col`", g.as("long_col_name", "col").str},
            {"`table`.`long_col_name` AS `col`", g.as("table", "long_col_name", "col").str},

            {" FROM `table1`", g.from("table1")},
            {" FROM `table1` AS `t`", g.from(g.as("table1", "t"))},
            {" FROM `table1`,`table2`", g.from("table1", "table2")},
            {" FROM `table1`,`table2`,`database`.`table`",
             g.from("table1", "table2", g.id("database", "table"))},

            // Subquery in FROM
            {"(SELECT `worker` FROM `workers`)", subQuery.str},
            {"(SELECT `worker` FROM `workers`) AS `worker_ids`",
             g.as(g.subQuery(subQueryText), "worker_ids").str},
            {"(SELECT `worker` FROM `workers`) AS `worker_ids`", g.as(subQuery, "worker_ids").str},

            {"SELECT `col1`", g.select("col1")},
            {"SELECT `col1`,`col2`", g.select("col1", "col2")},
            {"SELECT COUNT(*) AS `num`", g.select(g.as(Sql::COUNT_STAR, "num"))},
            {"SELECT `worker`,COUNT(*) AS `num`", g.select("worker", g.as(Sql::COUNT_STAR, "num"))},

            {" PARTITION (`p1`,`p2`)", g.inPartition(g.partId(1), g.partId(2))},
            {"SELECT `objectId`,`chunkId`,`subChunkId` FROM `Object_12345` PARTITION (`p12`)",
             g.select("objectId", "chunkId", "subChunkId") + g.from("Object_12345") +
                     g.inPartition(g.partId(12))},

            {" INTO OUTFILE '/tmp/file.csv' " + csv::Dialect().sqlOptions(), g.intoOutfile("/tmp/file.csv")},
            {"SELECT * INTO OUTFILE '/tmp/file.csv' " + csv::Dialect().sqlOptions(),
             g.select(Sql::STAR) + g.intoOutfile("/tmp/file.csv")},

            // CREATE DATABASE [IF NOT EXISTS] ...
            {"CREATE DATABASE `database`", g.createDb("database")},
            {"CREATE DATABASE IF NOT EXISTS `database`", g.createDb("database", true)},

            // DROP DATABASE [IF EXISTS] ...
            {"DROP DATABASE `database`", g.dropDb("database")},
            {"DROP DATABASE IF EXISTS `database`", g.dropDb("database", true)},
            {"DROP DATABASE `database`", g.dropDb(g.id("database"))},

            // ALTER TABLE ...
            {"ALTER TABLE `table`", g.alterTable("table")},
            {"ALTER TABLE `table` REMOVE PARTITIONING", g.alterTable("table", "REMOVE PARTITIONING")},
            {"ALTER TABLE `table`  REMOVE PARTITIONING", g.alterTable("table", g.removePartitioning())},
            {"ALTER TABLE `database`.`table`", g.alterTable(g.id("database", "table"))},
            {" REMOVE PARTITIONING", g.removePartitioning()},
            {" ADD PARTITION (PARTITION `p12` VALUES IN (12))", g.addPartition(12)},
            {" ADD PARTITION IF NOT EXISTS (PARTITION `p12` VALUES IN (12))", g.addPartition(12, true)},
            {" DROP PARTITION `p2`", g.dropPartition(2)},
            {" DROP PARTITION IF EXISTS `p3`", g.dropPartition(3, true)},

            // LOAD DATA [LOCAL] INFILE  ...
            {"LOAD DATA INFILE '/tmp/infile.csv' INTO TABLE `table` " + csv::Dialect().sqlOptions(),
             g.loadDataInfile("/tmp/infile.csv", "table")},
            {"LOAD DATA INFILE '/tmp/infile.csv' INTO TABLE `database`.`table` " +
                     csv::Dialect().sqlOptions(),
             g.loadDataInfile("/tmp/infile.csv", g.id("database", "table"))},
            {"LOAD DATA LOCAL INFILE '/tmp/infile.csv' INTO TABLE `table` " + csv::Dialect().sqlOptions(),
             g.loadDataInfile("/tmp/infile.csv", "table", "", true, csv::Dialect())},
            {"LOAD DATA INFILE '/tmp/infile.csv' INTO TABLE `table` CHARACTER SET 'latin1' " +
                     csv::Dialect().sqlOptions(),
             g.loadDataInfile("/tmp/infile.csv", "table", "latin1")},

            // GRANT ...
            {"GRANT ALL ON `db`.* TO 'qsreplica'@'localhost'",
             g.grant("ALL", "db", "qsreplica", "localhost")},
            {"GRANT SELECT,UPDATE,DELETE ON `db`.`table` TO 'qsreplica'@'127.0.0.1'",
             g.grant("SELECT,UPDATE,DELETE", "db", "table", "qsreplica", "127.0.0.1")},

            // Table indexes management
            {"CREATE UNIQUE INDEX `idx_worker_status` ON `workers` (`worker` ASC,`status` DESC) COMMENT "
             "'Unique composite index on workers and tables.'",
             g.createIndex("workers", "idx_worker_status", "UNIQUE", indexKeys,
                           "Unique composite index on workers and tables.")},
            {"CREATE INDEX `idx_worker` ON `db`.`workers` (`worker`(16) ASC) COMMENT 'Non-unique index on "
             "workers.'",
             g.createIndex(g.id("db", "workers"), "idx_worker", "", indexKeys2,
                           "Non-unique index on workers.")},

            {"SHOW INDEXES FROM `workers`", g.showIndexes("workers")},
            {"SHOW INDEXES FROM `db`.`workers`", g.showIndexes(g.id("db", "workers"))},

            {"DROP INDEX `idx_ObjectId` ON `table`", g.dropIndex("table", "idx_ObjectId")},
            {"DROP INDEX `idx_ObjectId` ON `db`.`table`", g.dropIndex(g.id("db", "table"), "idx_ObjectId")},

            {"SHOW WARNINGS", g.warnings()},
            {"SHOW WARNINGS LIMIT 64", g.warnings() + g.limit(64)},

            {"SHOW VARIABLES", g.showVars(SqlVarScope::SESSION)},
            {"SHOW GLOBAL VARIABLES", g.showVars(SqlVarScope::GLOBAL, "")},
            {"SHOW GLOBAL VARIABLES LIKE 'myisam_%'", g.showVars(SqlVarScope::GLOBAL, "myisam_%")},

            {"SET `var1`=1", g.setVars(SqlVarScope::SESSION, make_pair("var1", 1))},
            {"SET GLOBAL `var2`=2,`var3`='abc'",
             g.setVars(SqlVarScope::GLOBAL, make_pair("var2", 2), make_pair("var3", "abc"))}};

    for (auto&& test : tests) {
        BOOST_CHECK_EQUAL(test.first, test.second);
    }

    LOGS_INFO("QueryGenerator test ends");
}

BOOST_AUTO_TEST_SUITE_END()
