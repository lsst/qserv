/* Boost.MultiIndex basic example.
 *
 * Copyright 2003-2013 Joaquin M Lopez Munoz.
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * See http://www.boost.org/libs/multi_index for library home page.
 */

#if !defined(NDEBUG)
#define BOOST_MULTI_INDEX_ENABLE_INVARIANT_CHECKING
#define BOOST_MULTI_INDEX_ENABLE_SAFE_MODE
#endif

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <utility>

using boost::multi_index_container;
using namespace boost::multi_index;

namespace {

// a task record sored witin the index
struct task {
    int chunk;
    int qid;

    task(int chunk_, int qid_) : chunk(chunk_), qid(qid_) {}

    friend std::ostream& operator<<(std::ostream& os, const task& e) {
        os << e.chunk << " " << e.qid << std::endl;
        return os;
    }
};

// tags for accessing the corresponding indices of task_set

struct chunk {};
struct qid {};

/* see Compiler specifics: Use of member_offset for info on BOOST_MULTI_INDEX_MEMBER
 */

/* Define a multi_index_container of tasks with following indices:
 *   - a non-unique index sorted by task::int,
 *   - a non-unique index sorted by task::int.
 */
typedef multi_index_container<
        task, indexed_by<ordered_non_unique<tag<chunk>, BOOST_MULTI_INDEX_MEMBER(task, int, chunk)>,
                         ordered_non_unique<tag<qid>, BOOST_MULTI_INDEX_MEMBER(task, int, qid)>>>
        task_set;
#if 0
typedef multi_index_container<
        task,
        indexed_by<sequenced<>,
                   ordered_non_unique<tag<chunk>, BOOST_MULTI_INDEX_MEMBER(task, int, chunk)>,
                   sequenced<>,
                   ordered_non_unique<tag<qid>, BOOST_MULTI_INDEX_MEMBER(task, int, qid)> > >
        task_set;
#endif

template <typename Tag, typename MultiIndexContainer>
void print_out_by(const MultiIndexContainer& s) {
    // obtain a reference to the index tagged by Tag
    const typename boost::multi_index::index<MultiIndexContainer, Tag>::type& itr = get<Tag>(s);

    typedef typename MultiIndexContainer::value_type value_type;

    // dump the elements of the index to cout
    std::copy(itr.begin(), itr.end(), std::ostream_iterator<value_type>(std::cout));
}

int report_error(std::string const& msg) {
    std::cerr << msg << "\n"
              << "Usage: -d -c<chunks> -q<queries>" << std::endl;
    return 1;
}
}  // namespace

int main(int argc, char* argv[]) {
    bool use_multimap = false;
    bool dump = false;
    int numChunks = 1;
    int numQueries = 1;
    for (int argIdx = 1; argIdx < argc; ++argIdx) {
        std::string const arg = argv[argIdx];
        std::string const opt = arg.substr(0, 2);
        std::string const val = arg.substr(2);
        if ((opt == "-m") && val.empty()) {
            use_multimap = true;
        } else if ((opt == "-d") && val.empty()) {
            dump = true;
        } else if ((opt == "-c") && !val.empty()) {
            numChunks = std::stoi(val);
        } else if ((opt == "-q") && !val.empty()) {
            numQueries = std::stoi(val);
        } else {
            return ::report_error("error: unrecognized parameter, arg: '" + arg + "'");
        }
    }
    std::cout << "numChunks: " << numChunks << ", numQueries: " << numQueries
              << ", dump: " << (dump ? "1" : "0") << std::endl;

    if (use_multimap) {
        std::multimap<int, ::task, std::greater<int>> mm;
        for (int chunk = 0; chunk < numChunks; ++chunk) {
            for (int qid = 0; qid < numQueries; ++qid) {
                mm.insert(std::pair{chunk, ::task(chunk, qid)});
            }
        }
    } else {
        ::task_set es;
        ::task_set::iterator itr = es.begin();
        for (int chunk = 0; chunk < numChunks; ++chunk) {
            for (int qid = 0; qid < numQueries; ++qid) {
                es.insert(::task(chunk, qid));
            }
        }

        if (dump) {
            // list the tasks sorted by chunk and qid
            std::cout << "by chunk" << std::endl;
            print_out_by<::chunk>(es);
            std::cout << std::endl;

            std::cout << "by qid" << std::endl;
            print_out_by<::qid>(es);
            std::cout << std::endl;
        }
    }
    return 0;
}