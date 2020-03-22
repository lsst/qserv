add_library(mysql OBJECT)
add_dependencies(mysql proto)

target_sources(mysql PRIVATE
    LocalInfile.cc
    LocalInfile.h
    LocalInfileError.h
    MySqlConfig.cc
    MySqlConfig.h
    MySqlConnection.cc
    MySqlConnection.h
    RowBuffer.cc
    RowBuffer.h
    SchemaFactory.cc
    SchemaFactory.h
)

target_link_libraries(mysql PUBLIC
    log
    mysqlclient_r
)