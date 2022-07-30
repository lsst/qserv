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
    string const subQueryText = g.sqlSelect("worker") + g.sqlFrom("workers");
    auto const subQuery = g.sqlSubQuery(subQueryText);

    string valuesPackedIncrementally = g.sqlPackValues(1, "abc");
    g.sqlPackValue(valuesPackedIncrementally, false);
    g.sqlPackValue(valuesPackedIncrementally, 1.234567f);

    string columnsPackedIncrementally = g.sqlPackIds("col1", "col2");
    g.sqlPackId(columnsPackedIncrementally, string("col3"));
    g.sqlPackId(columnsPackedIncrementally, Function::LAST_INSERT_ID);

    string pairsPackedIncrementally =
            g.sqlPackPairs(make_pair("col1", "abc"), make_pair("col2", string("c")));
    g.sqlPackPair(pairsPackedIncrementally, make_pair("col3", 123));
    g.sqlPackPair(pairsPackedIncrementally, make_pair("col4", Function::LAST_INSERT_ID));

    string conditionsPackedIncrementally = g.sqlPackConditions();
    g.sqlPackCondition(conditionsPackedIncrementally, g.sqlEqual("col", 123));
    g.sqlPackCondition(conditionsPackedIncrementally, "");
    g.sqlPackCondition(conditionsPackedIncrementally, g.sqlIn("database", databases2));
    g.sqlPackCondition(conditionsPackedIncrementally, string());
    g.sqlPackCondition(conditionsPackedIncrementally, g.sqlIn("worker", workers2));

    // Column and key definitions for generating CREATE TABLE ... queries

    string const insertPacked =
            "INSERT INTO `Object` (`col1`,`col2`) VALUES (1,'abc') ON DUPLICATE KEY UPDATE "
            "`col1`=1,`col2`='abc'";

    list<SqlColDef> const columns = {
            {"id", "INT NOT NULL"}, {"col1", "VARCHAR(256) DEFAULT=''"}, {"col2", "DOUBLE"}};

    list<string> const keys = {g.sqlPackTableKey("PRIMARY KEY", "", "id"),
                               g.sqlPackTableKey("UNIQUE KEY", "composite", "col1", "col2")};

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
            {"`column`", g.sqlId("column").str},
            {"`db`.`table`", g.sqlId("db", "table").str},
            {"`table`.*", g.sqlId("table", Function::STAR).str},
            {"`p12`", g.sqlPartitionId(transactionId).str},
            {"DISTINCT `col`", g.sqlDistinctId("col").str},
            {"LAST_INSERT_ID()", Function::LAST_INSERT_ID.str},
            {"COUNT(*)", Function::COUNT_STAR.str},
            {"*", Function::STAR.str},
            {"DATABASE()", Function::DATABASE.str},
            {"NOW()", Function::NOW.str},
            {"UNIX_TIMESTAMP(`time`)", Function::UNIX_TIMESTAMP(g.sqlId("time")).str},
            {"UNIX_TIMESTAMP(`time`)", g.UNIX_TIMESTAMP("time").str},
            {"UNIX_TIMESTAMP(`table`.`column`)", g.UNIX_TIMESTAMP(g.sqlId("table", "column")).str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,NOW())",
             Function::TIMESTAMPDIFF("SECOND", g.sqlId("submitted"), Function::NOW).str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,`completed`)",
             Function::TIMESTAMPDIFF("SECOND", g.sqlId("submitted"), g.sqlId("completed")).str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,`completed`)",
             g.TIMESTAMPDIFF("SECOND", "submitted", "completed").str},
            {"TIMESTAMPDIFF(SECOND,`submitted`,NOW())",
             g.TIMESTAMPDIFF("SECOND", "submitted", Function::NOW).str},
            {"TIMESTAMPDIFF(SECOND,`table`.`submitted`,`table`.`completed`)",
             g.TIMESTAMPDIFF("SECOND", g.sqlId("table", "submitted"), g.sqlId("table", "completed")).str},

            // Values
            {"1", g.sqlValue(true).str},
            {"0", g.sqlValue(false).str},
            {"123", g.sqlValue(123).str},
            {"-123", g.sqlValue(-123).str},
            {"1.234567", g.sqlValue(1.234567f).str},
            {"'abc'", g.sqlValue("abc").str},
            {"'abc'", g.sqlValue(string("abc")).str},
            {"DO_NOT_PROCESS", g.sqlValue(DoNotProcess("DO_NOT_PROCESS")).str},
            {"SUM(`size`)", g.sqlValue(DoNotProcess("SUM(" + g.sqlId("size").str + ")")).str},
            {"NULL", g.sqlValue(Keyword::SQL_NULL).str},
            {"LAST_INSERT_ID()", g.sqlValue(Function::LAST_INSERT_ID).str},

            // Conditional injection of values
            {"'abc'", g.sqlValue(g.nullIfEmpty("abc")).str},
            {"NULL", g.sqlValue(g.nullIfEmpty("")).str},
            {"NULL", g.sqlValue(g.nullIfEmpty(string())).str},

            // Packing lists of values
            {"", g.sqlPackValues()},
            {"1", g.sqlPackValues(1)},
            {"''", g.sqlPackValues("")},
            {"''", g.sqlPackValues(string())},
            {"1,''", g.sqlPackValues(1, "")},
            {"1,0,123,-123,1.234567,'abc','abc',DO_NOT_PROCESS,NULL,LAST_INSERT_ID()",
             g.sqlPackValues(true, false, 123, -123, 1.234567f, "abc", string("abc"),
                             DoNotProcess("DO_NOT_PROCESS"), Keyword::SQL_NULL, Function::LAST_INSERT_ID)},
            {"1,'abc',0,1.234567", valuesPackedIncrementally},
            {" VALUES (1,'')", g.sqlValues(1, "")},

            {"(" + subQueryText + ")", subQuery.str},

            // Preparing complete IN or NOT IN clauses
            {"", g.sqlIn("worker", workers0)},
            {"`worker` IN ('worker-1','worker-2')", g.sqlIn("worker", workers2)},
            {"", g.sqlNotIn("worker", workers0)},
            {"`worker` NOT IN ('worker-1','worker-2')", g.sqlNotIn("worker", workers2)},

            // Preparing complete IN or NOT IN clauses with a subquery
            {"", g.sqlInSubQuery("worker", "")},
            {"`worker` IN " + subQuery.str, g.sqlInSubQuery("worker", subQuery)},
            {"`worker` IN (" + subQueryText + ")", g.sqlInSubQuery("worker", subQueryText)},
            {"`config`.`worker` IN " + subQuery.str, g.sqlInSubQuery(g.sqlId("config", "worker"), subQuery)},
            {"", g.sqlNotInSubQuery("worker", "")},
            {"`worker` NOT IN " + subQuery.str, g.sqlNotInSubQuery("worker", subQuery)},

            // Packed conditions that's used in the WHERE clause
            {"", g.sqlPackConditions()},
            {"`col`=123", g.sqlPackConditions(g.sqlEqual("col", 123))},
            {"`col`=123 AND `database` IN ('dbA','dbB')",
             g.sqlPackConditions(g.sqlEqual("col", 123), g.sqlIn("database", databases2))},
            {"", g.sqlPackConditions(g.sqlIn("database", databases0))},
            {"`col`=123 AND `database` NOT IN ('dbA','dbB')",
             g.sqlPackConditions(g.sqlEqual("col", 123), g.sqlNotIn("database", databases2))},
            {"`col`=123 AND `database` IN ('dbA','dbB') AND `worker` IN ('worker-1','worker-2')",
             conditionsPackedIncrementally},

            // Predicates to support searches using the FULL TEXT indexes
            {"MATCH(`query`) AGAINST('dp02' IN NATURAL LANGUAGE MODE)",
             g.sqlMatchAgainst("query", "dp02", "NATURAL LANGUAGE")},
            {"MATCH(`QInfo`.`query`) AGAINST('dp02' IN BOOLEAN MODE)",
             g.sqlMatchAgainst(g.sqlId("QInfo", "query"), "dp02", "BOOLEAN")},

            // Preparing complete WHERE clause
            {"", g.sqlWhere()},
            {" WHERE `col`=123", g.sqlWhere(g.sqlEqual("col", 123))},
            {" WHERE `col`=123 AND `database` IN ('dbA','dbB')",
             g.sqlWhere(g.sqlEqual("col", 123), g.sqlIn("database", databases2))},
            {"", g.sqlWhere(g.sqlIn("database", databases0))},
            {" WHERE `col`=123 AND `database` NOT IN ('dbA','dbB')",
             g.sqlWhere(g.sqlEqual("col", 123), g.sqlNotIn("database", databases2))},

            // Preparing complete WHERE clause with a sub-query
            {" WHERE `col`=123 AND `worker` NOT IN " + subQuery.str,
             g.sqlWhere(g.sqlEqual("col", 123), g.sqlNotInSubQuery("worker", subQuery))},

            // Binary operators
            {"`col`=123", g.sqlEqual("col", 123)},
            {"`col`=1", g.sqlEqual("col", true)},
            {"`col`='abc'", g.sqlEqual("col", "abc")},
            {"`col`=NULL", g.sqlEqual("col", Keyword::SQL_NULL)},
            {"`col`!=123", g.sqlNotEqual("col", 123)},
            {"`col`<123", g.sqlLess("col", 123)},
            {"`col`<=123", g.sqlLessOrEqual("col", 123)},
            {"`col`>123", g.sqlGreater("col", 123)},
            {"`col`>123", g.sqlGreater(g.sqlId("col"), 123)},
            {"UNIX_TIMESTAMP(`time`)>1234567890", g.sqlGreater(g.UNIX_TIMESTAMP("time"), 1234567890)},
            {"NOW()>1234567890", g.sqlGreater(Function::NOW, 1234567890)},
            {"NOW()>1234567890", g.sqlGreater(Function::NOW, 1234567890)},
            {"`col`>=123", g.sqlGreaterOrEqual("col", 123)},
            {"`col` REGEXP '[0-9]+'", g.sqlRegexp("col", "[0-9]+")},
            {"NOW()<=UNIX_TIMESTAMP(`time`)",
             g.sqlBinaryOperator(Function::NOW, g.UNIX_TIMESTAMP("time"), "<=")},
            {"NOW()=`time`", g.sqlBinaryOperator(Function::NOW, g.sqlId("time"), "=")},

            // Packed pairs for using in INSERT ... VALUES()
            {"", g.sqlPackPairs()},
            {"`col1`='abc',`col2`='c',`col3`=123,`col4`=LAST_INSERT_ID()",
             g.sqlPackPairs(make_pair("col1", "abc"), make_pair("col2", string("c")), make_pair("col3", 123),
                            make_pair("col4", Function::LAST_INSERT_ID))},
            {"`col1`='abc',`col2`='c',`col3`=123,`col4`=LAST_INSERT_ID()", pairsPackedIncrementally},

            // Preparing complete ORDER BY clause
            {"", g.sqlOrderBy()},
            {" ORDER BY `col1`", g.sqlOrderBy(make_pair("col1", ""))},
            {" ORDER BY `col1`", g.sqlOrderBy(make_pair(string("col1"), ""))},
            {" ORDER BY `col1`", g.sqlOrderBy(make_pair("col1", string()))},
            {" ORDER BY `col1`", g.sqlOrderBy(make_pair(string("col1"), string()))},
            {" ORDER BY `col1` DESC", g.sqlOrderBy(make_pair("col1", "DESC"))},
            {" ORDER BY `col1` ASC,`col2` DESC",
             g.sqlOrderBy(make_pair("col1", "ASC"), make_pair("col2", "DESC"))},
            {" ORDER BY `col1` ASC,`col2` DESC,`col3`",
             g.sqlOrderBy(make_pair("col1", "ASC"), make_pair("col2", "DESC"), make_pair("col3", ""))},

            // Pack collections of columns into strings
            {"", g.sqlPackIds()},
            {"`col1`", g.sqlPackIds("col1")},
            {"`col1`", g.sqlPackIds(string("col1"))},
            {"`col1`,`col2`", g.sqlPackIds("col1", "col2")},
            {"`col1`,`col2`,`col3`", g.sqlPackIds("col1", "col2", string("col3"))},
            {"LAST_INSERT_ID()", g.sqlPackIds(Function::LAST_INSERT_ID)},
            {"COUNT(*)", g.sqlPackIds(Function::COUNT_STAR)},
            {"*", g.sqlPackIds(Function::STAR)},
            {"`category`,COUNT(*)", g.sqlPackIds("category", Function::COUNT_STAR)},
            {"`col1`,`col2`,`col3`,LAST_INSERT_ID()", columnsPackedIncrementally},

            // Preparing complete GROUP BY clause
            {"", g.sqlGroupBy()},
            {" GROUP BY `col1`", g.sqlGroupBy("col1")},
            {" GROUP BY `col1`", g.sqlGroupBy(string("col1"))},
            {" GROUP BY `col1`,`col2`", g.sqlGroupBy("col1", "col2")},
            {" GROUP BY `col1`,`col2`,`col3`", g.sqlGroupBy("col1", "col2", "col3")},

            // Preparing complete LIMIT clause
            {"", g.sqlLimit(0)},
            {" LIMIT 123", g.sqlLimit(123)},

            // Complete INSERT queries
            {"INSERT INTO `Object` VALUES ()", g.sqlInsert("Object")},
            {"INSERT INTO `Object` VALUES (123456,'abc',1)", g.sqlInsert("Object", 123456, "abc", true)},
            {insertPacked,
             g.sqlInsertPacked("Object", g.sqlPackIds("col1", "col2"), g.sqlPackValues(1, "abc"),
                               g.sqlPackPairs(make_pair("col1", 1), make_pair("col2", "abc")))},

            // Complete UPDATE queries
            {"UPDATE `Object` SET `col1`='abc',`col2`=345",
             g.sqlUpdate("Object", make_pair("col1", "abc"), make_pair("col2", 345))},
            {"UPDATE `Object` SET `col1`='abc',`col2`=345 WHERE `id`=123",
             g.sqlUpdate("Object", make_pair("col1", "abc"), make_pair("col2", 345)) +
                     g.sqlWhere(g.sqlEqual("id", 123))},

            // Complete DELETE queries
            {"DELETE FROM `workers`", g.sqlDelete("workers")},
            {"DELETE FROM `config`.`workers` WHERE `is_offline`=1 AND `worker` IN ('worker-1','worker-2')",
             g.sqlDelete(g.sqlId("config", "workers")) +
                     g.sqlWhere(g.sqlEqual("is_offline", true), g.sqlIn("worker", workers2))},

            // Key generator to be used for generating CREATE TABLE queries
            {"PRIMARY KEY (`id`)", g.sqlPackTableKey("PRIMARY KEY", "", "id")},
            {"UNIQUE KEY `composite` (`col1`,`col2`)",
             g.sqlPackTableKey("UNIQUE KEY", "composite", "col1", "col2")},

            // CREATE TABLE ...
            {" PARTITION BY LIST (`qserv_trans_id`)", g.sqlPartitionByList("qserv_trans_id")},
            {" (PARTITION `p1` VALUES IN (1))", g.sqlPartition(1)},

            {createTableOne, g.sqlCreateTable("one", true, list<SqlColDef>())},
            {createTableOne, g.sqlCreateTable(g.sqlId("one"), true, list<SqlColDef>())},
            {createDbTableOne, g.sqlCreateTable("db", "one", false, list<SqlColDef>())},
            {createDbTableOne, g.sqlCreateTable(g.sqlId("db", "one"), false, list<SqlColDef>())},
            {createTableTwo,
             g.sqlCreateTable("two", false, columns, list<string>(), "MyISAM", "the comment")},
            {createTableThree, g.sqlCreateTable("three", false, columns, keys)},
            {createTableFour, g.sqlCreateTable("four", false, columns, keys, "MyISAM", "partitioned table") +
                                      g.sqlPartitionByList("qserv_trans_id") + g.sqlPartition(1)},

            {"CREATE TABLE `dst` LIKE `src`", g.sqlCreateTableLike("dst", "src")},
            {"CREATE TABLE IF NOT EXISTS `dst` LIKE `src`",
             g.sqlCreateTableLike(g.sqlId("dst"), g.sqlId("src"), true)},

            // DROP TABLE [IF EXISTS] ...
            {"DROP TABLE `table`", g.sqlDropTable("table")},
            {"DROP TABLE IF EXISTS `table`", g.sqlDropTable("table", true)},

            // REPLACE INTO ...
            {"REPLACE INTO `table` VALUES (1,'abc')", g.sqlReplace("", "table", 1, "abc")},
            {"REPLACE INTO `db`.`table` VALUES (1,'abc',LAST_INSERT_ID())",
             g.sqlReplace("db", "table", 1, "abc", Function::LAST_INSERT_ID)},

            // SELECT ...
            {"COUNT(*) AS `num`", g.sqlAs(Function::COUNT_STAR, "num").str},
            {"0 AS `id`", g.sqlAs(DoNotProcess("0"), "id").str},
            {"`long_col_name` AS `col`", g.sqlAs("long_col_name", "col").str},
            {"`table`.`long_col_name` AS `col`", g.sqlAs("table", "long_col_name", "col").str},

            {" FROM `table1`", g.sqlFrom("table1")},
            {" FROM `table1` AS `t`", g.sqlFrom(g.sqlAs("table1", "t"))},
            {" FROM `table1`,`table2`", g.sqlFrom("table1", "table2")},
            {" FROM `table1`,`table2`,`database`.`table`",
             g.sqlFrom("table1", "table2", g.sqlId("database", "table"))},

            // Subquery in FROM
            {"(SELECT `worker` FROM `workers`)", subQuery.str},
            {"(SELECT `worker` FROM `workers`) AS `worker_ids`",
             g.sqlAs(g.sqlSubQuery(subQueryText), "worker_ids").str},
            {"(SELECT `worker` FROM `workers`) AS `worker_ids`", g.sqlAs(subQuery, "worker_ids").str},

            {"SELECT `col1`", g.sqlSelect("col1")},
            {"SELECT `col1`,`col2`", g.sqlSelect("col1", "col2")},
            {"SELECT COUNT(*) AS `num`", g.sqlSelect(g.sqlAs(Function::COUNT_STAR, "num"))},
            {"SELECT `worker`,COUNT(*) AS `num`",
             g.sqlSelect("worker", g.sqlAs(Function::COUNT_STAR, "num"))},

            {" PARTITION (`p1`,`p2`)", g.sqlRestrictByPartition(g.sqlPartitionId(1), g.sqlPartitionId(2))},
            {"SELECT `objectId`,`chunkId`,`subChunkId` FROM `Object_12345` PARTITION (`p12`)",
             g.sqlSelect("objectId", "chunkId", "subChunkId") + g.sqlFrom("Object_12345") +
                     g.sqlRestrictByPartition(g.sqlPartitionId(12))},

            {" INTO OUTFILE '/tmp/file.csv' " + csv::Dialect().sqlOptions(),
             g.sqlIntoOutfile("/tmp/file.csv")},
            {"SELECT * INTO OUTFILE '/tmp/file.csv' " + csv::Dialect().sqlOptions(),
             g.sqlSelect(Function::STAR) + g.sqlIntoOutfile("/tmp/file.csv")},

            // CREATE DATABASE [IF NOT EXISTS] ...
            {"CREATE DATABASE `database`", g.sqlCreateDatabase("database")},
            {"CREATE DATABASE IF NOT EXISTS `database`", g.sqlCreateDatabase("database", true)},

            // DROP DATABASE [IF EXISTS] ...
            {"DROP DATABASE `database`", g.sqlDropDatabase("database")},
            {"DROP DATABASE IF EXISTS `database`", g.sqlDropDatabase("database", true)},
            {"DROP DATABASE `database`", g.sqlDropDatabase(g.sqlId("database"))},

            // ALTER TABLE ...
            {"ALTER TABLE `table`", g.sqlAlterTable("table")},
            {"ALTER TABLE `table` REMOVE PARTITIONING", g.sqlAlterTable("table", "REMOVE PARTITIONING")},
            {"ALTER TABLE `table`  REMOVE PARTITIONING", g.sqlAlterTable("table", g.sqlRemovePartitioning())},
            {"ALTER TABLE `database`.`table`", g.sqlAlterTable(g.sqlId("database", "table"))},
            {" REMOVE PARTITIONING", g.sqlRemovePartitioning()},
            {" ADD PARTITION (PARTITION `p12` VALUES IN (12))", g.sqlAddPartition(12)},
            {" ADD PARTITION IF NOT EXISTS (PARTITION `p12` VALUES IN (12))", g.sqlAddPartition(12, true)},
            {" DROP PARTITION `p2`", g.sqlDropPartition(2)},

            // LOAD DATA [LOCAL] INFILE  ...
            {"LOAD DATA INFILE '/tmp/infile.csv' INTO TABLE `table` " + csv::Dialect().sqlOptions(),
             g.sqlLoadDataInfile("/tmp/infile.csv", "table")},
            {"LOAD DATA INFILE '/tmp/infile.csv' INTO TABLE `database`.`table` " +
                     csv::Dialect().sqlOptions(),
             g.sqlLoadDataInfile("/tmp/infile.csv", g.sqlId("database", "table"))},
            {"LOAD DATA LOCAL INFILE '/tmp/infile.csv' INTO TABLE `table` " + csv::Dialect().sqlOptions(),
             g.sqlLoadDataInfile("/tmp/infile.csv", "table", true, csv::Dialect())},

            // GRANT ...
            {"GRANT ALL ON `db`.* TO 'qsreplica'@'localhost'",
             g.sqlGrant("ALL", "db", "qsreplica", "localhost")},
            {"GRANT SELECT,UPDATE,DELETE ON `db`.`table` TO 'qsreplica'@'127.0.0.1'",
             g.sqlGrant("SELECT,UPDATE,DELETE", "db", "table", "qsreplica", "127.0.0.1")},

            // Table indexes management
            {"CREATE UNIQUE INDEX `idx_worker_status` ON `workers` (`worker` ASC,`status` DESC) COMMENT "
             "'Unique composite index on workers and tables.'",
             g.sqlCreateIndex("workers", "idx_worker_status", "UNIQUE", indexKeys,
                              "Unique composite index on workers and tables.")},
            {"CREATE INDEX `idx_worker` ON `db`.`workers` (`worker`(16) ASC) COMMENT 'Non-unique index on "
             "workers.'",
             g.sqlCreateIndex(g.sqlId("db", "workers"), "idx_worker", "", indexKeys2,
                              "Non-unique index on workers.")},

            {"SHOW INDEXES FROM `workers`", g.sqlShowIndexes("workers")},
            {"SHOW INDEXES FROM `db`.`workers`", g.sqlShowIndexes(g.sqlId("db", "workers"))},

            {"DROP INDEX `idx_ObjectId` ON `table`", g.sqlDropIndex("table", "idx_ObjectId")},
            {"DROP INDEX `idx_ObjectId` ON `db`.`table`",
             g.sqlDropIndex(g.sqlId("db", "table"), "idx_ObjectId")}};

    for (auto&& test : tests) {
        BOOST_CHECK_EQUAL(test.first, test.second);
    }

    LOGS_INFO("QueryGenerator test ends");
}

BOOST_AUTO_TEST_SUITE_END()
