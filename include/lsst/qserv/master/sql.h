#ifndef LSST_QSERV_MASTER_SQL_H
#define LSST_QSERV_MASTER_SQL_H
// Not sure we should use this, since it will probably conflict
// with db usage via the python api. *grumble*


// Standard
#include <string>

// Boost
#include <boost/thread.hpp>
// MySQL
#include <mysql/mysql.h> // MYSQL is typedef, so we can't forward declare it.

namespace lsst {
namespace qserv {
namespace master {

class SqlConfig {
public:
    SqlConfig() : port(0) {}
    std::string hostname;
    std::string username;
    std::string password;
    std::string dbName;
    unsigned int port;
    std::string socket;
};
    
class SqlConnection {
public:
    SqlConnection(SqlConfig const& sc, bool useThreadMgmt=false); 
    ~SqlConnection(); 
    bool connectToDb();
    bool apply(std::string const& sql);

    char const* getMySqlError() const { return _mysqlError; }
    int getMySqlErrno() const { return _mysqlErrno; }
private:
    bool _init();
    bool _connect();
    void _discardResults(MYSQL* mysql);
    void _storeMysqlError(MYSQL* c);

    MYSQL* _conn;
    std::string _error;
    int _mysqlErrno;
    const char* _mysqlError;
    SqlConfig _config;
    bool _connected;
    bool _useThreadMgmt;
    static boost::mutex _sharedMutex;
    static bool _isReady;
}; // class SqlConnection


}}} // namespace lsst::qserv::master
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_MASTER_SQL_H
