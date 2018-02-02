#include <iostream>
#include <sstream>
#include <string>

namespace {

std::string sqlId (std::string const& val) {
    return "`" + val + "`";
}

// Value filtering and quotation (for string types)

template <typename T>
T sqlValue (T const&    val,
            char const* quote="'") {
    return val;
}
template <>
std::string sqlValue<std::string> (std::string const& val,
                                   char const*        quote) {
    return quote + val + quote;
}
std::string sqlValue (char const* val) {
    return sqlValue (std::string(val));
}


// The base (the final function) to be called
void sqlValues (std::string& sql) {
    sql += ")";
}

// Recursive variadic function
template <typename T, typename...Targs>
void sqlValues (std::string& sql, T val, Targs... Fargs) {
    std::ostringstream ss;
    std::cout << sizeof...(Fargs) << std::endl;
    ss << (sql.empty() ? "(" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlValue (val);
    sql += ss.str();
    sqlValues (sql, Fargs...);
}

template <typename...Targs>
std::string sqlPackValues (Targs... Fargs) {
    std::string sql;
    sqlValues (sql, Fargs...);
    return sql;
}



///////////////////////////////////////////////////////////////////////
// Generator:    [`column` = value [, `column` = value [, ... ]]]
///////////////////////////////////////////////////////////////////////

// The base (the final function) to be called
void sqlPackPair (std::string&) {}

// Forward declaration for the overloaded vrsion (not needed witghin a class)
template <typename T, typename...Targs>
void sqlPackPair (std::string&             sql,
                  std::pair<char const*,T> colVal,
                  Targs...                 Fargs);

// Recursive variadic function
template <typename T, typename...Targs>
void sqlPackPair (std::string&             sql,
                  std::pair<std::string,T> colVal,
                  Targs...                 Fargs) {

    std::string const& col = colVal.first;
    T const&           val = colVal.second;

    std::ostringstream ss;
    std::cout << sizeof...(Fargs) << std::endl;
    ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlId (col) << "=" << sqlValue (val);
    sql += ss.str();
    sqlPackPair (sql, Fargs...);
}

template <typename T, typename...Targs>
void sqlPackPair (std::string&             sql,
                  std::pair<char const*,T> colVal,
                  Targs...                 Fargs) {

    std::string const  col = colVal.first;
    T const&           val = colVal.second;

    std::ostringstream ss;
    std::cout << sizeof...(Fargs) << std::endl;
    ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlId (col) << "=" << sqlValue (val);
    sql += ss.str();
    sqlPackPair (sql, Fargs...);
}

template <typename...Targs>
std::string sqlPackPairs (Targs... Fargs) {
    std::string sql;
    sqlPackPair (sql, Fargs...);
    return sql;
}

} // namespace

int main (int argc, char const* argv[]) {

    std::cout <<
        ::sqlPackValues (
            "str",
            std::string("c"),
            123,
            24.5) << std::endl;

    std::cout <<
        ::sqlPackPairs (
            std::make_pair ("col1", "1")) << std::endl;

    std::cout <<
        ::sqlPackPairs (
            std::make_pair (            "col1",  "1"),
            std::make_pair (std::string("col2"), "2")) << std::endl;

    std::cout <<
        ::sqlPackPairs (
            std::make_pair (std::string("col1"), "1")) << std::endl;

    std::cout <<
        ::sqlPackPairs (
            std::make_pair (std::string("col1"), "1"),
            std::make_pair (            "col2",  "2"),
            std::make_pair (            "col3",   3)) << std::endl;

    std::cout <<
        ::sqlPackPairs (
            std::make_pair (std::string("col1"), "1"),
            std::make_pair (            "col2",  "2"),
            std::make_pair (std::string("col3"),  3)) << std::endl;

    return 0;
}