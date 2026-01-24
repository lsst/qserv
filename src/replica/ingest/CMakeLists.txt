add_library(replica_ingest OBJECT)

add_dependencies(replica_ingest replica_proto)

target_sources(replica_ingest PRIVATE
    IngestClient.cc
    IngestDataHttpSvcMod.cc
    IngestFileHttpSvcMod.cc
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
    IngestUtils.cc
    TransactionContrib.cc
)

target_link_libraries(replica_ingest PUBLIC
    log
)
