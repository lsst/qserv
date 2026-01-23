add_library(partition SHARED)

target_sources(partition PRIVATE
    Chunker.cc
    ChunkIndex.cc
    ChunkReducer.cc
    CmdLineUtils.cc
    ConfigStore.cc
    Csv.cc
    FileUtils.cc
    Geometry.cc
    HtmIndex.cc
    InputLines.cc
    ObjectIndex.cc
    ParquetInterface.cc
    PartitionTool.cc
    PartitionMatchesTool.cc
)

target_link_libraries(partition PUBLIC
    boost_program_options
    boost_thread
    Threads::Threads
    arrow
    parquet
    log
)

install(TARGETS partition)

#----------------------------------------------------------------

FUNCTION(partition_apps)
    FOREACH(APP IN ITEMS ${ARGV})
        add_executable(${APP} ${APP}.cc)
        target_link_libraries(${APP} PRIVATE
            partition
            boost_filesystem
            sphgeom
            arrow
            parquet
        )
        install(TARGETS ${APP})
    ENDFOREACH()
ENDFUNCTION()

partition_apps(
    sph-duplicate
    sph-duplicate2
    sph-estimate-stats
    sph-htm-index
    sph-layout
    sph-parq2csv
    sph-partition-matches
    sph-partition
)

#----------------------------------------------------------------

add_subdirectory(tests)
