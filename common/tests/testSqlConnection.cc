

#include "lsst/qserv/SqlConnection.hh"

int main(int, char**) {
    std::stringstream ss;

    lsst::qserv::SqlConfig sc;
    sc.hostname = "";
    sc.username = "becla";
    sc.password = "";
    sc.dbName = "";
    sc.port = 0;
    sc.socket = "/var/run/mysqld/mysqld.sock";
    
    lsst::qserv::SqlConnection _sqlConn(sc);

    std::string dbN1 = "one_xysdfed34d";
    std::string dbN2 = "two_xysdfed34d";
    std::string dbN3 = "three_xysdfed34d";

    lsst::qserv::SqlErrorObject errObj;

    // this database should not exist
    assert( ! _sqlConn.dbExists(dbN1, errObj) );
    
    // create it now
    assert( ! _sqlConn.createDb(dbN1, errObj) );
    
    // this database should exist now
    assert( _sqlConn.dbExists(dbN1, errObj) );

    std::string tNa = "object_a";
    std::string tNb = "source_b";
    std::string tNc = "object_c";

    // check if table tN exists in default db
    assert ( ! _sqlConn.tableExists(tNa, errObj) );

    // create a different database now
    assert( ! _sqlConn.createDb(dbN2, errObj) );

    // check if table tN exists in dbN1
    assert ( ! _sqlConn.tableExists(tNa, errObj, dbN1) );

    // check if table tN exists in dbN2
    assert ( ! _sqlConn.tableExists(tNa, errObj, dbN2) );
    
    // create table in dbN1
    ss.str("");
    ss <<  "CREATE TABLE " << tNa << " (i int)";
    assert ( _sqlConn.apply(ss.str(), errObj) );

    // check if table tN exists in dbN1 (it should)
    assert ( _sqlConn.tableExists(tNa, errObj, dbN1) );

    // check if table tN exists in dbN2 (it should not)
    assert ( ! _sqlConn.tableExists(tNa, errObj, dbN2) );

    // switch to database dbN2
    assert ( _sqlConn.selectDb(dbN2, errObj) );
    
    // switch to database dbN3 (should fail)
    assert ( ! _sqlConn.selectDb(dbN2, errObj) );

    // create table tNa, tNb and tNc in dbN2
    ss.str(""); ss << "CREATE TABLE " << tNa << " (f float)";
    assert ( _sqlConn.apply(ss.str(), errObj) );

    ss.str(""); ss << "CREATE TABLE " << tNb << " (c char)";
    assert ( _sqlConn.apply(ss.str(), errObj) );

    ss.str(""); ss << "CREATE TABLE " << tNb << " (s char[3])";
    assert ( _sqlConn.apply(ss.str(), errObj) );

    std::vector<std::string> v;
    // list tables in dbN1 (should return one name)
    assert (! _sqlConn.listTables(v, errObj, "", dbN1));

    // list tables in dbN2 (should return three names)
    assert (! _sqlConn.listTables(v, errObj, "", dbN2));

    // list object tables in dbN2 (should return two names)
    assert (! _sqlConn.listTables(v, errObj, "object_", dbN2));

    // list source tables in dbN2 (should return one name)
    assert (! _sqlConn.listTables(v, errObj, "source_", dbN2));

    // list tables in dbN3 (should fail)
    assert (! _sqlConn.listTables(v, errObj));

    // drop dbN1 and dbN2
    assert ( _sqlConn.dropDb(dbN1, errObj) );
    assert ( _sqlConn.dropDb(dbN2, errObj) );

    // drop dbN3 (should fail)
    assert ( ! _sqlConn.dropDb(dbN3, errObj) );

    return 0;
}

