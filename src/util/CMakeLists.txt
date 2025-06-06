add_library(util SHARED)

target_sources(util PRIVATE
    AsyncTimer.cc
    BlockPost.cc
    Bug.cc
    CmdLineParser.cc
    Command.cc
    common.cc
    ConfigStore.cc
    ConfigValMap.cc
    DynamicWorkQueue.cc
    Error.cc
    EventThread.cc
    File.cc
    FileMonitor.cc
    Histogram.cc
    HoldTrack.cc
    InstanceCount.cc
    Issue.cc
    MultiError.cc
    Mutex.cc
    ResultFileName.cc
    StringHash.cc
    String.cc
    Substitution.cc
    TablePrinter.cc
    ThreadPool.cc
    ThreadPriority.cc
    Timer.cc
    TimeUtils.cc
    WorkQueue.cc
    xrootd.cc
)

target_link_libraries(util PUBLIC
    boost_filesystem
    boost_system
    log
)

install(
    TARGETS util
)

FUNCTION(util_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            crypto
            util
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

util_tests(
    testCommon
    testConfigStore
    testEventThread
    testResultFileName
    testIterableFormatter
    testHistogram
    testMultiError
    testMutex
    testString
    testTablePrinter
)
