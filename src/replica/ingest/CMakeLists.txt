add_library(replica_ingest OBJECT)
target_sources(replica_ingest PRIVATE
    IngestClient.cc
    IngestFileSvc.cc
    IngestHttpSvc.cc
    IngestHttpSvcMod.cc
    IngestRequest.cc
    IngestRequestMgr.cc
    IngestResourceMgr.cc
    IngestResourceMgrP.cc
    IngestResourceMgrT.cc
    IngestSvc.cc
    IngestSvcConn.cc
    TransactionContrib.cc
)
target_link_libraries(replica_ingest PUBLIC
    log
)
