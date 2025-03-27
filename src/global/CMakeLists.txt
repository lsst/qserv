add_library(global SHARED)

target_sources(global PRIVATE
    DbTable.cc
    debugUtil.cc
    ResourceUnit.cc
    sqltoken.cc
    stringUtil.cc
)

install(
	TARGETS global
)

target_link_libraries(global PUBLIC
    log
)
