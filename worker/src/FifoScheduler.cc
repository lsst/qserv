#include "lsst/qserv/worker/FifoScheduler.h"
#include <boost/thread.hpp>

namespace qWorker = lsst::qserv::worker;
typedef qWorker::Foreman::TaskQueuePtr TaskQueuePtr;

qWorker::FifoScheduler::FifoScheduler() 
    : _maxRunning(4) // FIXME: set to system proc count.
{

}    

TaskQueuePtr qWorker::FifoScheduler::nopAct(TodoList::Ptr todo, 
                                            TaskQueuePtr running) {
    // For now, do nothing when there is no event.  

    // Perhaps better: Check to see how many are running, and schedule
    // a task if the number of running jobs is below a threshold.
    return TaskQueuePtr();
}

TaskQueuePtr qWorker::FifoScheduler::newTaskAct(Task::Ptr incoming,
                                                TodoList::Ptr todo, 
                                                TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    TaskQueuePtr tq;
    assert(running.get());
    assert(todo.get());
    assert(incoming.get());
    if(running->size() < _maxRunning) { // if we have space, start
                                      // running.
        // Prefer tasks already in the todo list, although there
        // shouldn't be... 
        tq.reset(new TodoList::TaskQueue());
        if(todo->size() > 0) {
            tq->push_back(todo->popTask());
        } else {
            tq->push_back(todo->popTask(incoming));
        }
        return tq;
    }
    return TaskQueuePtr();
}

TaskQueuePtr qWorker::FifoScheduler::taskFinishAct(Task::Ptr finished,
                                                   TodoList::Ptr todo, 
                                                   TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    TaskQueuePtr tq;
    assert(running.get());
    assert(todo.get());
    assert(finished.get());

    // FIFO always replaces a finishing task with a new task, always
    // maintaining a constant number of running threads (as long as
    // there is work to do)
    if(todo->size() > 0) {
        tq.reset(new TodoList::TaskQueue());
        tq->push_back(todo->popTask());
        return tq;
    } 
    // No more work to do--> don't schedule anything.
    return TaskQueuePtr();
}    
