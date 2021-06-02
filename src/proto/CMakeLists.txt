find_package(Protobuf REQUIRED)
protobuf_generate_cpp(PROTO_PB_SRCS PROTO_PB_HDRS worker.proto)

add_library(proto OBJECT)

target_compile_options(proto PRIVATE
    -Wno-maybe-uninitialized
)

target_sources(proto PRIVATE
    ${PROTO_PB_SRCS}
    ${PROTO_PB_HDRS}
    FrameBuffer.cc
    FrameBuffer.h
    ProtoHeaderWrap.cc
    ProtoHeaderWrap.h
    ProtoImporter.h
    ScanTableInfo.cc
    ScanTableInfo.h
    TaskMsgDigest.cc
    TaskMsgDigest.h
    WorkerResponse.h
)

target_link_libraries(proto PUBLIC
    log
    protobuf
)


add_executable(testProtocol testProtocol.cc)
target_link_libraries(testProtocol
    crypto
    ssl
    proto
    Boost::unit_test_framework
)
add_test(NAME testProtocol COMMAND testProtocol)
