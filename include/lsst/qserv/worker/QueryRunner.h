
#include "XrdSfs/XrdSfsInterface.hh"
#include "lsst/qserv/worker/Base.h"
namespace lsst {
namespace qserv {
namespace worker {

class ExecEnv {
public:
    std::string const& getSocketFilename() const { return _socketFilename; }
    std::string const& getMysqldumpPath() const { return _mysqldumpPath; }
private:
    ExecEnv() : _isReady(false){}
    void _setup();
    
    bool _isReady;
    // trim this list.
    std::string _userName;
    std::string _dumpName;
    std::string _socketFilename;
    std::string _mysqldumpPath;
    std::string _script;

    friend ExecEnv& getExecEnv();
};

ExecEnv& getExecEnv();

class QueryRunner {
public:
    QueryRunner(XrdOucErrInfo& ei, XrdSysError& e, 
		std::string const& user, ScriptMeta s,
		std::string overrideDump=std::string());
    bool operator()();

private:
    bool _runScript(std::string const& script, std::string const& dbName);
    std::string _runScriptPiece(MYSQL*const db,
				std::string const& scriptId, 
				std::string const& pieceName,
				std::string const& piece);

    std::string _runScriptPieces(MYSQL*const db,
				 std::string const& scriptId, 
				 std::string const& build, 
				 std::string const& run, 
				 std::string const& cleanup);



    ExecEnv& _env;
    XrdOucErrInfo& _errinfo;
    XrdSysError& _e;
    std::string _user;
    ScriptMeta _meta;
};

 int dumpFileOpen(std::string const& dbName);
 bool dumpFileExists(std::string const& dumpFilename);





}}}
