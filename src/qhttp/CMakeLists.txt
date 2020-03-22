add_library(qhttp SHARED)

target_sources(qhttp PRIVATE
    AjaxEndpoint.cc
    AjaxEndpoint.h
    Path.cc
    Path.h
    Request.cc
    Request.h
    Response.cc
    Response.h
    Server.cc
    Server.h
    StaticContent.cc
    StaticContent.h
)

target_link_libraries(qhttp PUBLIC
    boost_filesystem
    boost_regex
    boost_system
)

install(
    TARGETS qhttp
)
