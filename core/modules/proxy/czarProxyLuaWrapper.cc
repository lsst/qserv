
// System headers

// Third-party headers
extern "C" {
#include "lua.h"
}

// LSST headers

// Qserv headers
#include "proxy/czarProxy.h"

namespace {

int luaInitCzar(lua_State *L);
int luaSubmitQuery(lua_State *L);
int luaKillQuery(lua_State *L);
int luaLog(lua_State *L);

/*
 * Main entry point for lua.
 */
extern "C"
int
luaopen_czarProxy(lua_State* L) {

    lua_newtable(L); // Create module table

    // register this module's functions
    struct {
        const char* name;
        lua_CFunction fun;
    } methods[] = {
            {"initCzar", luaInitCzar},
            {"submitQuery", luaSubmitQuery},
            {"killQuery", luaKillQuery},
            {"log", luaLog}
    };

    for (auto&& method: methods) {
        lua_pushcfunction(L, method.fun);
        lua_setfield(L, -2, method.name);
    }

    // register module in globals
    lua_pushvalue(L, -1);
    lua_setglobal(L, "czarProxy");

    return 1;
}

int luaInitCzar(lua_State *L) {
    try {
        if (lua_gettop(L) != 1) {
            lua_pushstring(L, "One argument expected in initCzar(name)");
            lua_error(L);   // lua_error() does not return
        }
        if (!lua_isstring(L, -1)) {
            lua_pushstring(L, "incorrect argument type, expect string");
            lua_error(L);   // lua_error() does not return
        }
        size_t lenVal;
        const char* val = lua_tolstring(L, -1, &lenVal);
        lsst::qserv::proxy::initCzar(std::string(val, lenVal));
        return 0;
    } catch (std::exception const& exc) {
        lua_pushstring(L, exc.what());
        lua_error(L);   // lua_error() does not return
        return 0;
    }
}

int luaSubmitQuery(lua_State *L) {
    // called as submitQuery(query:str, hints:table) -> table
    try {
        if (lua_gettop(L) != 2) {
            lua_pushstring(L, "Two arguments expected in submitQuery(query:str, hints:table)");
            lua_error(L);   // lua_error() does not return
        }
        if (!lua_istable(L, -1) or !lua_isstring(L, -2)) {
            lua_pushstring(L, "submitQuery(query:str, hints:table) -- incorrect argument type");
            lua_error(L);   // lua_error() does not return
        }
        size_t lenQuery;
        const char* query = lua_tolstring(L, -2, &lenQuery);

        // copy table to map
        std::map<std::string, std::string> hints;
        lua_pushnil(L);  /* first key */
        while (lua_next(L, -2) != 0) {
            /* uses 'key' (at index -2) and 'value' (at index -1) */
            if(!(lua_isstring(L, -2) && lua_isstring(L, -1))) {
                lua_pushstring(L, "submitQuery(query:str, hints:table) - incorrect type in hints table");
                lua_error(L);   // lua_error() does not return
            }
            size_t lenKey, lenVal;
            const char* key = lua_tolstring(L, -2, &lenKey);
            const char* val = lua_tolstring(L, -1, &lenVal);
            hints.insert(std::make_pair(std::string(key, lenKey), std::string(val, lenVal)));
            lua_pop(L, 1);  // removes 'value'; keeps 'key' for next iteration
        }

        lsst::qserv::czar::SubmitResult res = lsst::qserv::proxy::submitQuery(std::string(query, lenQuery), hints);

        // convert result to table
        lua_newtable(L);
        lua_pushlstring (L, res.errorMessage.data(), res.errorMessage.size());
        lua_setfield(L, -2, "errorMessage");
        lua_pushlstring (L, res.resultTable.data(), res.resultTable.size());
        lua_setfield(L, -2, "resultTable");
        lua_pushlstring (L, res.messageTable.data(), res.messageTable.size());
        lua_setfield(L, -2, "messageTable");
        lua_pushlstring (L, res.orderBy.data(), res.orderBy.size());
        lua_setfield(L, -2, "orderBy");

        return 1;

    } catch (std::exception const& exc) {
        lua_pushstring(L, exc.what());
        lua_error(L);   // lua_error() does not return
        return 0;
    }
}

int luaKillQuery(lua_State *L) {
    // called as killQuery(query:str, clientId:str) -> str
    try {
        if (lua_gettop(L) != 2) {
            lua_pushstring(L, "Two arguments expected in killQuery(query:str, clientId:str)");
            lua_error(L);   // lua_error() does not return
        }
        if (!lua_isstring(L, -1) or !lua_isstring(L, -2)) {
            lua_pushstring(L, "killQuery(query:str, clientId:str) -- incorrect argument type");
            lua_error(L);   // lua_error() does not return
        }
        size_t lenQuery, lenClientId;
        const char* query = lua_tolstring(L, -2, &lenQuery);
        const char* clientId = lua_tolstring(L, -1, &lenClientId);

        auto msg = lsst::qserv::proxy::killQuery(std::string(query, lenQuery),
                std::string(clientId, lenClientId));

        lua_pushlstring(L, msg.data(), msg.size());
        return 1;

    } catch (std::exception const& exc) {
        lua_pushstring(L, exc.what());
        lua_error(L);   // lua_error() does not return
        return 0;
    }
}

int luaLog(lua_State *L) {
    // called as log(logger:str, level:str, message:str)
    try {
        if (lua_gettop(L) != 3) {
            lua_pushstring(L, "Three arguments expected in log(logger, level, message)");
            lua_error(L);   // lua_error() does not return
        }
        if (!lua_isstring(L, -1) or !lua_isstring(L, -2) or !lua_isstring(L, -3)) {
            lua_pushstring(L, "incorrect argument type, expect string");
            lua_error(L);   // lua_error() does not return
        }
        size_t lenLogger, lenLevel, lenMsg;
        const char* logger = lua_tolstring(L, -3, &lenLogger);
        const char* level = lua_tolstring(L, -2, &lenLevel);
        const char* message = lua_tolstring(L, -1, &lenMsg);

        // get file name, function name, and line number from stack
        std::string filename;
        std::string funcname;
        lua_Debug ar;
        lua_getstack(L, 1, &ar);
        lua_getinfo(L, "nSl", &ar);

        if (ar.source) {
            filename = ar.source;
            // strip some of the file path to make it shorter
            std::string::size_type p = filename.rfind('/');
            if (p != 0) p = filename.rfind('/', p-1);
            if (p != std::string::npos) filename.erase(0, p+1);
        }
        if (ar.name) funcname = ar.name;
        unsigned lineno = ar.currentline;

        lsst::qserv::proxy::log(std::string(logger, lenLogger), std::string(level, lenLevel),
                filename, funcname, lineno, std::string(message, lenMsg));
        return 0;
    } catch (std::exception const& exc) {
        lua_pushstring(L, exc.what());
        lua_error(L);   // lua_error() does not return
        return 0;
    }
}

} // namespace
