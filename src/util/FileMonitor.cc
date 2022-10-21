// -*- LSST-C++ -*-
/*
 * LSST Data Management System
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

// Class header
#include "util/FileMonitor.h"

// System headers
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/inotify.h>

// LSST headers
#include "lsst/log/Log.h"

// Project headers
#include "util/Bug.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.util");

}

#define EVENT_SIZE sizeof(struct inotify_event)
#define EVENT_BUF_LEN 1024 * (EVENT_SIZE + 16)

using namespace std;

namespace lsst::qserv::util {

FileMonitor::~FileMonitor() {
    _stop();
    _join();
}

void FileMonitor::_setup() {
    LOGS(_log, LOG_LVL_WARN, "FileMonitor::_setup() " << _fileName);
    _fD = inotify_init();
    if (_fD < 0) {
        throw Bug(ERR_LOC, "FileMonitor::setup inotify_init failed " + _fileName);
    }
    _wD = inotify_add_watch(_fD, _fileName.c_str(),
                            IN_CREATE | IN_MODIFY | IN_MOVE_SELF | IN_MOVED_FROM | IN_MOVED_TO);
}

void FileMonitor::_checkLoop() {
    while (_loop) {
        char buffer[EVENT_BUF_LEN];

        /// There's a lock situation here. If the file is never modified, it's never getting past
        /// this line. xrootd doesn't exit gracefully anyway, so this is unlikely to cause a problem.
        /// This thread could be cancelled or the file could be touched, but that's unlikely to make
        /// program termination much prettier.
        int length = read(_fD, buffer, EVENT_BUF_LEN);
        LOGS(_log, LOG_LVL_WARN, "FileMonitor::checkLoop() " << _fileName << " read length=" << length);
        if (length < 0) {
            LOGS(_log, LOG_LVL_ERROR, "FileMonitor::checkLoop length read=" << length);
            // Something bad happened, but crashing the program is probably not useful.
            continue;
        }

        int i = 0;
        while (i < length) {
            struct inotify_event *event = (struct inotify_event *)&buffer[i];
            LOGS(_log, LOG_LVL_DEBUG, "FileMonitor inotify event i=" << i << " event len=" << event->len);
            bool reread = false;
            string msg = "FileMonitor::checkLoop got event " + to_string(event->mask);
            if (event->mask & IN_CREATE) {
                msg += " IN_CREATE";
                reread = true;
            }
            if (event->mask & IN_MODIFY) {
                msg += " IN_MODIFY";
                reread = true;
            }
            if (event->mask & IN_MOVE_SELF) {
                msg += " IN_MOVE_SELF";
                reread = true;
            }
            if (event->mask & IN_MOVED_FROM) {
                msg += " IN_MOVED_FROM";
                // There's probably nothing there to read right now.
            }
            if (event->mask & IN_MOVED_TO) {
                msg += " IN_MOVED_TO";
                reread = true;
            }
            LOGS(_log, LOG_LVL_ERROR, msg << " reread=" << reread);
            // Only reload if the loop is still active and reread is true.
            if (reread && _loop) {
                LOGS(_log, LOG_LVL_ERROR, msg << " reloading config " << _fileName);
                sleep(1);  // Give it a second in hopes of log message being written before changes.
                LOG_CONFIG(_fileName);
            }
            i += EVENT_SIZE + event->len;
        }
    }

    // Close the watch and file descriptors.
    LOGS(_log, LOG_LVL_WARN, "FileMonitor::checkLoop() end " << _fileName);
    inotify_rm_watch(_fD, _wD);
    close(_fD);
}

void FileMonitor::run() {
    thread t(&FileMonitor::_checkLoop, this);
    _thrd = move(t);
}

void FileMonitor::_join() {
    if (_thrd.joinable()) {
        _thrd.join();
    } else {
        LOGS(_log, LOG_LVL_ERROR, "FileMonitor::join called when _thrd was not joinable.");
    }
}

}  // namespace lsst::qserv::util
