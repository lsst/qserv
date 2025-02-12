add_library(cconfig SHARED)

target_sources(cconfig PRIVATE
    CzarConfig.cc
)


target_link_libraries(cconfig PUBLIC
    log
)

install(
	TARGETS cconfig
)

install(
    TARGETS cconfig
)
