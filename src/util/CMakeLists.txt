add_library(util OBJECT)

target_sources(util PRIVATE
    BlockPost.cc
    BlockPost.h
    CmdLineParser.cc
    CmdLineParser.h
    Command.cc
    Command.h
    common.h
    ConfigStore.cc
    ConfigStore.h
    ConfigStoreError.h
    DynamicWorkQueue.cc
    DynamicWorkQueue.h
    Error.cc
    Error.h
    EventThread.cc
    EventThread.h
    File.cc
    File.h
    format.h
    InstanceCount.cc
    InstanceCount.h
    Issue.cc
    Issue.h
    IterableFormatter.h
    MultiError.cc
    MultiError.h
    Mutex.cc
    Mutex.h
    PointerCompare.h
    Regex.h
    SemaMgr.cc
    SemaMgr.h
    StringHash.cc
    StringHash.h
    StringHelper.cc
    StringHelper.h
    Substitution.cc
    Substitution.h
    TablePrinter.cc
    TablePrinter.h
    ThreadPool.cc
    ThreadPool.h
    ThreadPriority.cc
    ThreadPriority.h
    threadSafe.h
    Timer.cc
    Timer.h
    WorkQueue.cc
    WorkQueue.h
    xrootd.cc
    xrootd.h
)

target_link_libraries(util PUBLIC
    log
)


add_executable(testCommon testCommon.cc)
target_link_libraries(testCommon
    crypto
    util
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testCommon COMMAND testCommon)


add_executable(testEventThread testEventThread.cc)
target_link_libraries(testEventThread
    crypto
    util
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testEventThread COMMAND testEventThread)


add_executable(testIterableFormatter testIterableFormatter.cc)
target_link_libraries(testIterableFormatter
    crypto
    util
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testIterableFormatter COMMAND testIterableFormatter)


add_executable(testMultiError testMultiError.cc)
target_link_libraries(testMultiError
    crypto
    util
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testMultiError COMMAND testMultiError)


add_executable(testMutex testMutex.cc)
target_link_libraries(testMutex
    crypto
    util
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testMutex COMMAND testMutex)


add_executable(testTablePrinter testTablePrinter.cc)
target_link_libraries(testTablePrinter
    crypto
    util
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testTablePrinter COMMAND testTablePrinter)
