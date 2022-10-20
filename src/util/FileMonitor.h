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
#ifndef LSST_QSERV_UTIL_FILEMONITOR_H
#define LSST_QSERV_UTIL_FILEMONITOR_H

// System headers
#include <atomic>
#include <thread>


namespace lsst::qserv::util {

/// Bug is a generic Qserv exception that indicates a probable bug
class FileMonitor {
public:
	using Ptr = std::shared_ptr<FileMonitor>;

	FileMonitor() = delete;
	FileMonitor(std::string const& fileName) : _fileName(fileName) {
		setup();
	}

	/// Stops and joins _thrd, if joinable.
	~FileMonitor();

	FileMonitor(FileMonitor const&) = delete;
	FileMonitor& operator=(FileMonitor const&) = delete;

	void setup();

	void checkLoop();
	void run();
	void stop() { _loop = false; }
	void join();

private:
	std::string _fileName; ///< name of the file being watched, including path.

	std::thread _thrd; ///< Thread monitoring file changes
	std::atomic<bool> _loop{true};

	int _fD; ///< File descriptor for the monitor itself.
	int _wD; ///< inotify watch descriptor.
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_FILEMONITOR_H
