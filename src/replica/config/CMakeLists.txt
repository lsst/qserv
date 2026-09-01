add_library(replica_config OBJECT)
target_sources(replica_config PRIVATE
    ConfigCzar.cc
    ConfigDatabase.cc
    ConfigDatabaseFamily.cc
    ConfigHost.cc
    ConfigParserJSON.cc
    ConfigParserMySQL.cc
    ConfigTable.cc
    ConfigTestDataController.cc
    ConfigTestDataRegistry.cc
    ConfigTestDataWorker.cc
    ConfigWorker.cc
    Configuration.cc
    ConfigurationSchema.cc
    ConfigurationSchemaController.cc
    ConfigurationSchemaRegistry.cc
    ConfigurationSchemaWorker.cc
)
target_link_libraries(replica_config PUBLIC
    log
    sphgeom
)