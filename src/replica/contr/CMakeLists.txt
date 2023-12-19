add_library(replica_contr OBJECT)
target_sources(replica_contr PRIVATE
    Controller.cc
    DeleteWorkerTask.cc
    EventLogger.cc
    HealthMonitorTask.cc
    HttpCatalogsModule.cc
    HttpConfigurationModule.cc
    HttpControllersModule.cc
    HttpDirectorIndexModule.cc
    HttpExportModule.cc
    HttpIngestChunksModule.cc
    HttpIngestConfigModule.cc
    HttpIngestModule.cc
    HttpIngestTransModule.cc
    HttpJobsModule.cc
    HttpModule.cc
    HttpProcessor.cc
    HttpQservMonitorModule.cc
    HttpQservSqlModule.cc
    HttpReplicationLevelsModule.cc
    HttpRequestsModule.cc
    HttpSqlIndexModule.cc
    HttpSqlSchemaModule.cc
    HttpWorkerStatusModule.cc
    ReplicationTask.cc
    Task.cc
)
target_link_libraries(replica_contr PUBLIC
    log
    sphgeom
)
