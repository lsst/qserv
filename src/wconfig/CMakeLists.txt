add_library(wconfig OBJECT)
add_dependencies(wconfig proto)

target_sources(wconfig PRIVATE
    WorkerConfig.cc
    WorkerConfig.h
    WorkerConfigError.h
)

target_link_libraries(wconfig PUBLIC
    log
)