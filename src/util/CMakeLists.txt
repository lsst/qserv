add_library(util OBJECT)

target_sources(util PRIVATE
    BlockPost.cc
    Bug.cc
    CmdLineParser.cc
    Command.cc
    ConfigStore.cc
    DynamicWorkQueue.cc
    Error.cc
    EventThread.cc
    File.cc
    FileMonitor.cc
    Histogram.cc
    InstanceCount.cc
    Issue.cc
    MultiError.cc
    Mutex.cc
    SemaMgr.cc
    StringHash.cc
    StringHelper.cc
    Substitution.cc
    TablePrinter.cc
    ThreadPool.cc
    ThreadPriority.cc
    Timer.cc
    WorkQueue.cc
    xrootd.cc
)

target_link_libraries(util PUBLIC
    log
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
    testEventThread
    testIterableFormatter
    testHistogram
    testMultiError
    testMutex
    testTablePrinter
)
