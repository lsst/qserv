#include "lsst/qserv/worker/FifoScheduler.h"

namespace qWorker = lsst::qserv::worker;
typedef qWorker::Foreman::TaskQueuePtr TaskQueuePtr;

qWorker::FifoScheduler::FifoScheduler() {
}    

TaskQueuePtr qWorker::FifoScheduler::nopAct(TodoList::Ptr todo, 
                                            TaskQueuePtr running) {
    return TaskQueuePtr();
}

TaskQueuePtr qWorker::FifoScheduler::newTaskAct(Task::Ptr incoming,
                                                TodoList::Ptr todo, 
                                                TaskQueuePtr running) {
    return TaskQueuePtr();
}

TaskQueuePtr qWorker::FifoScheduler::taskFinishAct(Task::Ptr finished,
                                                   TodoList::Ptr todo, 
                                                   TaskQueuePtr running) {
    return TaskQueuePtr();
}    
