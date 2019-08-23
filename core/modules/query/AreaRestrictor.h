// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_AREARESTRICTOR_H
#define LSST_QSERV_QUERY_AREARESTRICTOR_H


// System headers
#include <memory>
#include <string>
#include <vector>


// Forward declarations
namespace lsst {
namespace sphgeom {
    class Region;
}
namespace qserv {
namespace query {
    class BoolFactor;
    class QueryTemplate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// AreaRestrictor is a Qserv spatial restrictor element that is used to signal dependencies on
/// spatially-partitioned tables that make use of spatial indexing.

class AreaRestrictor {
public:
    AreaRestrictor() = default;
    virtual ~AreaRestrictor() = default;

    bool operator==(const AreaRestrictor& rhs) const;

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    virtual void renderTo(QueryTemplate& qt) const = 0;

    /**
     * @brief Serialize to the given ostream for debug output.
     */
    virtual std::ostream& dbgPrint(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os, AreaRestrictor const& q);

    /**
     * @brief Get the scisql function that is equivalent to the area reatrictor, for the given table
     * and chunk columns.
     *
     * @param tableAlias The alias of the table to put in the scisql function.
     * @param chunkColumns The columns names in the table to use in the scisql function.
     * @return std::shared_ptr<query::BoolFactor> The Qserv IR element that describes the scisql function.
     */
    virtual std::shared_ptr<query::BoolFactor> asSciSqlFactor(std::string const& tableAlias,
            std::pair<std::string, std::string> const& chunkColumns) const = 0;

    /**
     * @brief Get the Region for this area spec object.
     *
     * @return std::shared_ptr<sphgeom::Region>
     */
    virtual std::shared_ptr<sphgeom::Region> getRegion() const = 0;

protected:
    /**
     * @brief Test if this is equal with rhs.
     *
     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    virtual bool isEqual(const AreaRestrictor& rhs) const = 0;
};


class AreaRestrictorBox : public AreaRestrictor{
public:
    AreaRestrictorBox(std::string const& lonMinDegree,
                      std::string const& latMinDegree,
                      std::string const& lonMaxDegree,
                      std::string const& latMaxDegree);

    AreaRestrictorBox(std::vector<std::string> const& parameters);

    void renderTo(QueryTemplate& qt) const override;

    std::ostream& dbgPrint(std::ostream& os) const override;

    std::shared_ptr<query::BoolFactor> asSciSqlFactor(std::string const& tableAlias,
            std::pair<std::string, std::string> const& chunkColumns) const override;

    std::shared_ptr<sphgeom::Region> getRegion() const override;

protected:
    bool isEqual(const AreaRestrictor& rhs) const override;

private:
    std::string _lonMinDegree;
    std::string _latMinDegree;
    std::string _lonMaxDegree;
    std::string _latMaxDegree;
};


class AreaRestrictorCircle : public AreaRestrictor{
public:
    virtual ~AreaRestrictorCircle() = default;

    AreaRestrictorCircle(std::string const& centerLonDegree,
                         std::string const& centerLatDegree,
                         std::string const& radiusDegree);

    AreaRestrictorCircle(std::vector<std::string> const& parameters);

    void renderTo(QueryTemplate& qt) const override;

    std::ostream& dbgPrint(std::ostream& os) const override;

    std::shared_ptr<query::BoolFactor> asSciSqlFactor(std::string const& tableAlias,
            std::pair<std::string, std::string> const& chunkColumns) const override;

    std::shared_ptr<sphgeom::Region> getRegion() const override;

protected:
    bool isEqual(const AreaRestrictor& rhs) const override;

private:
    std::string _centerLonDegree;
    std::string _centerLatDegree;
    std::string _radiusDegree;
};


class AreaRestrictorEllipse : public AreaRestrictor{
public:
    virtual ~AreaRestrictorEllipse() = default;

    AreaRestrictorEllipse(std::string const& centerLonDegree,
                          std::string const& centerLatDegree,
                          std::string const& semiMajorAxisAngleArcsec,
                          std::string const& semiMinorAxisAngleArcsec,
                          std::string const& positionAngleDegree);

    AreaRestrictorEllipse(std::vector<std::string> const& parameters);

    void renderTo(QueryTemplate& qt) const override;

    std::ostream& dbgPrint(std::ostream& os) const override;

    std::shared_ptr<query::BoolFactor> asSciSqlFactor(std::string const& tableAlias,
            std::pair<std::string, std::string> const& chunkColumns) const override;

    std::shared_ptr<sphgeom::Region> getRegion() const override;

protected:
    bool isEqual(const AreaRestrictor& rhs) const override;

private:
    std::string _centerLonDegree;
    std::string _centerLatDegree;
    std::string _semiMajorAxisAngleArcsec;
    std::string _semiMinorAxisAngleArcsec;
    std::string _positionAngleDegree;
};


class AreaRestrictorPoly : public AreaRestrictor{
public:
    virtual ~AreaRestrictorPoly() = default;

    AreaRestrictorPoly(std::vector<std::string> const& parameters);

    void renderTo(QueryTemplate& qt) const override;

    std::ostream& dbgPrint(std::ostream& os) const override;

    std::shared_ptr<query::BoolFactor> asSciSqlFactor(std::string const& tableAlias,
            std::pair<std::string, std::string> const& chunkColumns) const override;

    std::shared_ptr<sphgeom::Region> getRegion() const override;

protected:
    bool isEqual(const AreaRestrictor& rhs) const override;

private:
    std::vector<std::string> _parameters;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_AREARESTRICTOR_H
