add_library(query OBJECT)
add_dependencies(query proto)

target_sources(query PRIVATE
    AggOp.cc
    AggOp.h
    AggRecord.h
    AndTerm.cc
    AndTerm.h
    AreaRestrictor.cc
    AreaRestrictor.h
    BetweenPredicate.cc
    BetweenPredicate.h
    BoolFactor.cc
    BoolFactor.h
    BoolFactorTerm.h
    BoolTerm.cc
    BoolTermFactor.cc
    BoolTermFactor.h
    BoolTerm.h
    ChunkMapping.cc
    ChunkMapping.h
    ColumnRef.cc
    ColumnRef.h
    CompPredicate.cc
    CompPredicate.h
    CopyTerms.h
    FromList.cc
    FromList.h
    FuncExpr.cc
    FuncExpr.h
    GenericPredicate.cc
    GenericPredicate.h
    GroupByClause.cc
    GroupByClause.h
    HavingClause.cc
    HavingClause.h
    InPredicate.cc
    InPredicate.h
    JoinRef.cc
    JoinRef.h
    JoinSpec.cc
    JoinSpec.h
    LikePredicate.cc
    LikePredicate.h
    LogicalTerm.cc
    LogicalTerm.h
    NullPredicate.cc
    NullPredicate.h
    OrderByClause.cc
    OrderByClause.h
    OrTerm.cc
    OrTerm.h
    PassListTerm.cc
    PassListTerm.h
    PassTerm.cc
    PassTerm.h
    Predicate.cc
    Predicate.h
    QueryContext.cc
    QueryContext.h
    QueryTemplate.cc
    QueryTemplate.h
    SecIdxRestrictor.cc
    SecIdxRestrictor.h
    SelectList.cc
    SelectList.h
    SelectStmt.cc
    SelectStmt.h
    SubsetHelper.h
    TableRef.cc
    TableRef.h
    TestFactory.cc
    TestFactory.h
    typedefs.h
    UnknownTerm.cc
    UnknownTerm.h
    ValueExpr.cc
    ValueExpr.h
    ValueFactor.cc
    ValueFactor.h
    WhereClause.cc
    WhereClause.h
)

target_link_libraries(query PUBLIC
    log
    sphgeom
)

FUNCTION(QUERY_TESTS)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            query
            ccontrol
            parser
            qana
            qdisp
            qproc
            qserv_css
            qserv_meta
            rproc
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

query_tests(
    testAreaRestrictor
    testColumnRef
    testOrderBy
    testQueryContext
    testRepr
    testSecIdxRestrictor
    testTableRef
    testValueExpr
)
