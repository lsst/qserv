#ifndef LSST_QSERV_MASTER_SQL_H
#define LSST_QSERV_MASTER_SQL_H

// Standard
#include <string>

// MySQL
#include <mysql/mysql.h> // MYSQL is typedef, so we can't forward declare it.

namespace lsst {
namespace qserv {
namespace master {

class SqlConnection {
public:
    SqlConnection() : _conn(NULL), _port(0) { }
    ~SqlConnection(); 
    bool connectToResultDb();
    bool apply(std::string const& sql);

private:
    bool _init();
    bool _connect();
    void _discardResults(MYSQL* mysql);
    void _storeMysqlError(MYSQL* c);

    MYSQL* _conn;
    std::string _error;
    int _mysqlErrno;
    const char* _mysqlError;
    std::string _hostname;
    std::string _username;
    std::string _password;
    std::string _dbName;
    unsigned int _port;
    std::string _socket;
}; // class SqlConnection


}}} // namespace lsst::qserv::master
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_MASTER_SQL_H
