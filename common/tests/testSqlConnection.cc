

#include "SqlConnection.hh"

int main(int, char**) {

    SqlConfig sc;
    sc.hostname = 0;
    sc.username = "becla";
    sc.password = 0;
    sc.dbName = 0;
    sc.port = 0;
    sc.socket = '/var/run/mysqld/mysqld.sock';
    
    SqlConnection _sqlConn(sc);

    std::string dbN1 = "one_xysdfed34d";
    std::string dbN2 = "two_xysdfed34d";
    std::string dbN3 = "three_xysdfed34d";

    // this database should not exist
    assert( ! _sqlConn.dbExists(dbN1) );
    
    // create it now
    assert( ! _sqlConn.createDb(dbN1) );
    
    // this database should exist now
    assert( _sqlConn.dbExists(dbN1) );

    std::string tNa = "object_a";
    std::string tNb = "source_b";
    std::string tNc = "object_c";

    // check if table tN exists in default db
    assert ( ! _sqlConn.tableExists(tNa) );

    // create a different database now
    assert( ! _sqlConn.createDb(dbN2) );

    // check if table tN exists in dbN1
    assert ( ! _sqlConn.tableExists(tNa, dbN1) );

    // check if table tN exists in dbN2
    assert ( ! _sqlConn.tableExists(tNa, dbN2) );
    
    // create table in dbN1
    assert ( _sqlConn.apply("CREATE TABLE " + tNa " (i int)") );

    // check if table tN exists in dbN1 (it should)
    assert ( _sqlConn.tableExists(tNa, dbN1) );

    // check if table tN exists in dbN2 (it should not)
    assert ( ! _sqlConn.tableExists(tNa, dbN2) );

    // switch to database dbN2
    assert ( selectDb(dbN2) );
    
    // switch to database dbN3 (should fail)
    assert ( ! selectDb(dbN2) );

    // create table tNa, tNb and tNc in dbN2
    assert ( _sqlConn.apply("CREATE TABLE " + tNa " (f float)") );
    assert ( _sqlConn.apply("CREATE TABLE " + tNb " (c char)") );
    assert ( _sqlConn.apply("CREATE TABLE " + tNc " (s char[3])") );

    // list tables in dbN1 (should return one name)
    assert (! _sqlConn.listTables("", dbN1));

    // list tables in dbN2 (should return three names)
    assert (! _sqlConn.listTables("", dbN2));

    // list object tables in dbN2 (should return two names)
    assert (! _sqlConn.listTables("object_", dbN2));

    // list source tables in dbN2 (should return one name)
    assert (! _sqlConn.listTables("source_", dbN2));

    // list tables in dbN3 (should fail)
    assert (! _sqlConn.listTables());

    // drop dbN1 and dbN2
    assert ( _sqlConn.dropDb(dbN1) );
    assert ( _sqlConn.dropDb(dbN2) );

    // drop dbN3 (should fail)
    assert ( ! _sqlConn.dropDb(dbN3) );

    return 0;
}

