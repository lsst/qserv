/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
 * 
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the LSST License Statement and 
 * the GNU General Public License along with this program.  If not, 
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
 
#ifndef LSST_QSERV_WORKER_RESULT_TRACKER_H
#define LSST_QSERV_WORKER_RESULT_TRACKER_H
#include <deque>
#include <iostream>

#include <boost/signal.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

namespace lsst {
namespace qserv {
namespace worker {

typedef std::pair<int,char const*> ResultItem;
typedef std::pair<int, std::string> ResultError;
typedef boost::shared_ptr<ResultError> ResultErrorPtr;


// Make sure Item is a primitive that can be copied.
template <typename Key, typename Item>
class ResultTracker {
public:
    ////////////////////////////////////////////////////
    // Typedefs and inner classes
    typedef boost::signal<void (Item)> Signal;
    struct LockableSignal {
    public:
	typedef std::deque<boost::signals::connection> CDeque;
	boost::mutex mutex;
	Signal signal;
	CDeque connections; // Remember connections so we can disconnect.
	void clearListeners() {
	    CDeque::iterator i, end;
	    for(i=connections.begin(), end=connections.end();
		i != end; ++i) {
		i->disconnect();
	    }
	    
	}
    };
    ////////////////////////////////////////////////////
    typedef boost::shared_ptr<Item> ItemPtr;
    typedef boost::shared_ptr<LockableSignal> LSPtr;
    typedef std::map<Key, LSPtr> SignalMap;
    typedef std::map<Key, Item> NewsMap;
    //////////////////////////////////////////////////
    // Methods
    void notify(Key const& k, Item const& i) {
	_verifyKey(k); // Force k to exist in _signals
	LSPtr s = _signals[k];
	{	
	    boost::unique_lock<boost::mutex> slock(s->mutex);
	    //std::cerr << "Callback (tracker) signalling " 
	    //	      << k << " ---- " << std::endl;
	    s->signal(i); // Notify listeners
	    s->clearListeners();
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
		//std::cerr << "Callback reuse Chance --1--" << std::endl;
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
		//std::cerr << "Callback reuse Chance --2--" << std::endl;
		Callable c2(c);
		c2(i->second); 
	    } else {
		// No news, so subscribe.
		s->connections.push_back(s->signal.connect(c)); 
	    }
	}
    }
    ItemPtr getNews(Key const& k) {
	ItemPtr p;
	typename NewsMap::iterator i = _news.find(k);
	if(i != _news.end()) { 
	    p = boost::make_shared<Item>(i->second);
	}
	return p;
    }

    int getNewsCount() const {
	return _news.size(); // 
    }
    int getSignalCount() const {
	return _signals.size();
    }
    // Debug methods
    NewsMap& debugGetNews() { return _news; }
    SignalMap& debugGetSignals() { return _signals; }
    void debugReset() {
	_signals.clear();
	_news.clear();
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

    SignalMap _signals;
    NewsMap _news;
    boost::mutex _signalsMutex;
    boost::mutex _newsMutex;

};
    
}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_RESULT_TRACKER_H
