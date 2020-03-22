find_package(XRootD REQUIRED)

add_library(wdb OBJECT)
add_dependencies(wdb proto)

target_sources(wdb PRIVATE
    ChunkResource.cc
    ChunkResource.h
    QueryRunner.cc
    QueryRunner.h
    QuerySql_Batch.h
    QuerySql.cc
    QuerySql.h
    SQLBackend.cc
    SQLBackend.h
)

target_include_directories(wdb PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(wdb PUBLIC
    log
    XrdSsiLib
)
