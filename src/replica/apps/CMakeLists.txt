add_library(replica_apps OBJECT)
add_dependencies(replica_apps replica_proto)
target_include_directories(replica_apps PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)
target_sources(replica_apps PRIVATE
    AbortTransactionApp.cc
    AdminApp.cc
    Application.cc
    ApplicationColl.cc
    ApplicationTypes.cc
    TestAwsS3App.cc
    CheckSumApp.cc
    ChunksApp.cc
    ClusterHealthApp.cc
    ConfigApp.cc
    ConfigAppBase.cc
    ConfigTestApp.cc
    ControllerApp.cc
    DatabaseTestApp.cc
    DeleteWorkerApp.cc
    DirectorIndexApp.cc
    FileIngestApp.cc
    FileReadApp.cc
    FileServerApp.cc
    FixUpApp.cc
    HttpAsyncReqApp.cc
    HttpClientApp.cc
    HttpLibServerApp.cc
    MasterControllerHttpApp.cc
    MessengerTestApp.cc
    MoveApp.cc
    MySQLTestApp.cc
    PurgeApp.cc
    QhttpTestApp.cc
    QservWorkerApp.cc
    QservWorkerPingApp.cc
    QservXrootdSsiApp.cc
    RebalanceApp.cc
    RegistryHttpApp.cc
    ReplicateApp.cc
    SqlApp.cc
    SyncApp.cc
    TransactionsApp.cc
    VerifyApp.cc
    WorkerApp.cc
)
target_link_libraries(replica_apps PUBLIC
    log
    XrdSsiLib
)
