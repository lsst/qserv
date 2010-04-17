#ifndef LSST_QSERV_SQL_PARSER_BASE
#define LSST_QSERV_SQL_PARSER_BASE
#include "antlr/CommonAST.hpp"

// Abstract types to be used in the SQL grammar.
// * Not placed in include/ 
// * Placed with DmlSQL2.g and SqlSQL2.g, which generate .cpp and .hpp
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
class VoidTwoRefFunc {
public: 
    virtual ~VoidTwoRefFunc() {}
    virtual void operator()(RefAST a, RefAST b) = 0;
};


#endif // LSST_QSERV_SQL_PARSER_BASE

