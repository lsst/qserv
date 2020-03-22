add_library(qana OBJECT)
add_dependencies(qana proto)

target_sources(qana PRIVATE
    AggregatePlugin.cc
    AggregatePlugin.h
    AnalysisError.h
    CheckAggregation.h
    ColumnVertexMap.cc
    ColumnVertexMap.h
    DuplSelectExprPlugin.cc
    DuplSelectExprPlugin.h
    InvalidTableError.h
    MatchTablePlugin.cc
    MatchTablePlugin.h
    PluginNotFoundError.h
    PostPlugin.cc
    PostPlugin.h
    QservRestrictorPlugin.cc
    QservRestrictorPlugin.h
    QueryMapping.cc
    QueryMapping.h
    QueryNotEvaluableError.h
    QueryPlugin.h
    RelationGraph.cc
    RelationGraph.h
    ScanTablePlugin.cc
    ScanTablePlugin.h
    TableInfo.cc
    TableInfo.h
    TableInfoPool.cc
    TableInfoPool.h
    TablePlugin.cc
    TablePlugin.h
    WherePlugin.cc
    WherePlugin.h
)

target_link_libraries(qana PUBLIC
    log
)
