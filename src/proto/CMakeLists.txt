add_library(proto SHARED)

target_compile_options(proto PRIVATE
    -Wno-maybe-uninitialized
)

protobuf_generate_cpp(PROTO_PB_SRCS PROTO_PB_HDRS worker.proto)

target_sources(proto PRIVATE
    ${PROTO_PB_SRCS}
    ${PROTO_PB_HDRS}
    FrameBuffer.cc
    ProtoHeaderWrap.cc
    ScanTableInfo.cc
)

target_link_libraries(proto PUBLIC
    log
    protobuf
)

install(
    TARGETS proto
)

add_executable(testProtocol testProtocol.cc)

target_link_libraries(testProtocol
    proto
    crypto
    Boost::unit_test_framework
)

add_test(NAME testProtocol COMMAND testProtocol)

