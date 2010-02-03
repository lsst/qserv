#ifndef LSST_QSERV_WORKER_RESULT_TRACKER_H
#define LSST_QSERV_WORKER_RESULT_TRACKER_H
#include <boost/signal.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
namespace lsst {
namespace qserv {
namespace worker {

typedef std::pair<int,char const*> ResultItem;

// Make sure Item is a primitive that can be copied.
template <typename Key, typename Item>
class ResultTracker {
public:
    typedef boost::signal<void (Item)> Signal;

    void notify(Key const& k, Item const& i) {
	_verifyKey(k); // Force k to exist in _signals
	LSPtr s = _signals[k];
	{	
	    boost::unique_lock<boost::mutex> slock(s->mutex);
	    s->signal(i); // Notify listeners
	    // Get rid of listeners by constructing anew.
	    s->signal = typename ResultTracker<Key,Item>::Signal(); 
	    // Save news item
	    boost::unique_lock<boost::mutex> nlock(_newsMutex);
	    _news[k] = i;
	}
    } 
    void clearNews(Key const& k) {
	boost::unique_lock<boost::mutex> lock(_newsMutex);
	typename NewsMap::iterator i = _news.find(k);
	    if(i != _news.end()) {
		_news.erase(i);
	    }
    }
    template <typename Callable>
    void listenOnce(Key const& k, Callable const& c) {
	{ // This block is an optional optimization.
	    boost::unique_lock<boost::mutex> lock(_newsMutex);
	    typename NewsMap::iterator i = _news.find(k);
	    if(i != _news.end()) { // If already reported, reuse.
		Callable c2(c);
		c2(i->second);
		return; // No need to actually subscribe.
	    }
	}
	_verifyKey(k);
	LSPtr s = _signals[k];
	{	
	    boost::unique_lock<boost::mutex> lock(s->mutex);
	    // Check again, in case there was a notification.
	    typename NewsMap::iterator i = _news.find(k);
	    if(i != _news.end()) { 
		Callable c2(c);
		c2(i->second); 
	    } else {
		// No news, so subscribe.
		s->signal.connect(c); 
	    }
	}
    }
    int getNewsCount() const {
	return _news.size();
    }
    int getSignalCount() const {
	return _signals.size();
    }
private:
    void _verifyKey(Key const& k) {
	if(_signals.find(k) == _signals.end()) {
	    boost::unique_lock<boost::mutex> lock(_signalsMutex);
	    // Double-check within the mutex.
	    if(_signals.find(k) == _signals.end()) {
		_signals[k] = boost::make_shared<LockableSignal>();
	    }
	}	
    }

    struct LockableSignal {
    public:
	boost::mutex mutex;
	Signal signal;
    };
    typedef boost::shared_ptr<LockableSignal> LSPtr;
    typedef std::map<Key, LSPtr> SignalMap;
    typedef std::map<Key, Item> NewsMap;
    SignalMap _signals;
    NewsMap _news;
    boost::mutex _signalsMutex;
    boost::mutex _newsMutex;

};
    
}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_RESULT_TRACKER_H
