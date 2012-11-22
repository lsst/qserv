/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

#ifndef TEMPFILE_H
#define TEMPFILE_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include <stdexcept>

#include "boost/filesystem/path.hpp"
#include "boost/shared_ptr.hpp"

#include "FileUtils.h"

namespace {
    struct TempFile {
        char name[7];
        int fd;

        TempFile() : name(), fd(-1) {
            strcpy(name, "XXXXXX");
            fd = mkstemp(name);
            if (fd == -1) {
                throw std::runtime_error("Failed to create temporary file.");
            }
        }

        ~TempFile() {
            if (fd != -1) {
                unlink(name);
                close(fd);
            }
        }

        boost::filesystem::path const path() const {
            return boost::filesystem::path(name);
        }

        void concatenate(TempFile const & t1, TempFile const & t2) {
            using lsst::qserv::admin::dupr::InputFile;
            using lsst::qserv::admin::dupr::OutputFile;

            InputFile if1(t1.path());
            InputFile if2(t2.path());
            OutputFile of(path(), true);
            size_t sz = static_cast<size_t>(std::max(if1.size(), if2.size()));
            boost::shared_ptr<void> buf(malloc(sz), free);
            if (!buf) {
                throw std::bad_alloc();
            }
            if1.read(buf.get(), 0, if1.size());
            of.append(buf.get(), static_cast<size_t>(if1.size()));
            if2.read(buf.get(), 0, if2.size());
            of.append(buf.get(), static_cast<size_t>(if2.size()));
        }
    };
}

#endif // TEMPFILE_H
