add_library(wcontrol SHARED)
add_dependencies(wcontrol proto)

target_sources(wcontrol PRIVATE
    Foreman.cc
    ResourceMonitor.cc
    SqlConnMgr.cc
    WorkerStats.cc
    WCzarInfoMap.cc
)

install(
	TARGETS wcontrol
)


target_link_libraries(wcontrol PUBLIC
    log
    qhttp
)
