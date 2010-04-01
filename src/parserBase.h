#ifndef LSST_SQL_PARSER_HELPER
#define LSST_SQL_PARSER_HELPER
#include "antlr/CommonAST.hpp"
#include "boost/shared_ptr.hpp"
class VoidFourRefFunc {
public: 
    virtual ~VoidFourRefFunc() {}
    virtual void operator()(RefAST a, RefAST b, RefAST c, RefAST d) = 0;
};
class VoidThreeRefFunc {
public: 
    virtual ~VoidThreeRefFunc() {}
    virtual void operator()(RefAST a, RefAST b, RefAST c) = 0;
};


#endif // LSST_SQL_PARSER_HELPER

