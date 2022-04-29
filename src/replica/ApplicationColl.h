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
#ifndef LSST_QSERV_REPLICA_APPLICATIONCOLL_H
#define LSST_QSERV_REPLICA_APPLICATIONCOLL_H

// System headers
#include <map>
#include <memory>
#include <string>

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ApplicationColl represents a collection of application launchers. It's meant
 * to simplify building command-line tools launching applications by names. This makes
 * it possible to avoid building a separate binary for each known application.
 */
class ApplicationColl {
public:
    ApplicationColl() = default;
    ApplicationColl(ApplicationColl const&) = default;
    ApplicationColl& operator=(ApplicationColl const&) = default;
    ~ApplicationColl() = default;

    /**
     * Register an application of the given type.
     * @param name The name of the application.
     */
    template <class APPLICATION>
    void add(std::string const& name) {
        _coll.insert(std::make_pair(name, std::make_shared<AppLauncher<APPLICATION>>()));
    }

    /**
     * Find an application and run it.
     * @note The name of the application is expected to be specified as the very
     *   first (argv[1]) mandatory parameter found in the input collection of
     *   arguments. This is required to be one of the names used during application
     *   registration when calling above described method add().
     * @param argc The number of the command line arguments.
     * @param argv A collection of the arguments.
     */
    int run(int argc, char* argv[]) const;

private:
    /// @param err The optional message to be printed onto the standard error stream
    void _printUsage(std::string const& err = std::string()) const;

    class AppLauncherBase {
    public:
        virtual int run(int argc, char* argv[]) const = 0;
    };

    template <class APPLICATION>
    class AppLauncher : public AppLauncherBase {
    public:
        virtual int run(int argc, char* argv[]) const override {
            auto const app = APPLICATION::create(argc, argv);
            return app->run();
        }
    };
    std::map<std::string, std::shared_ptr<AppLauncherBase>> _coll;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_APPLICATIONCOLL_H
