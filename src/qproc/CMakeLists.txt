add_library(qproc OBJECT)
add_dependencies(qproc proto)

target_sources(qproc PRIVATE
    ChunkQuerySpec.cc
    ChunkQuerySpec.h
    ChunkSpec.cc
    ChunkSpec.h
    DatabaseModels.cc
    DatabaseModels.h
    fakeGeometry.h
    geomAdapter.h
    IndexMap.cc
    IndexMap.h
    QueryProcessingBug.h
    QueryProcessingError.h
    QuerySession.cc
    QuerySession.h
    SecondaryIndex.cc
    SecondaryIndex.h
    TaskMsgFactory.cc
    TaskMsgFactory.h
)

target_link_libraries(qproc PRIVATE
    log
    sphgeom
)
