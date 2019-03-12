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

/**
 * @see DatabaseTestApp
 */

// System headers
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseTestApp.h"

using namespace std;
using namespace lsst::qserv::replica;

int main(int argc, char* argv[]) {
    try {
        auto const app = DatabaseTestApp::create(argc, argv);
        return app->run();
    } catch (exception const& ex) {
        cerr << "main()  the application failed, exception: " << ex.what() << endl;
        return 1;
    }
}
