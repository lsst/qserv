// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 LSST Corporation.
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
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_QSRESTRICTOR_H
#define LSST_QSERV_QUERY_QSRESTRICTOR_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "global/stringTypes.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class BetweenPredicate;
    class ColumnRef;
    class CompPredicate;
    class QueryTemplate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// QsRestrictor is a Qserv spatial restrictor element that is used to
/// signal dependencies on spatially-partitioned tables. It includes
/// qserv-specific restrictors that make use of the spatial indexing,
/// but are not strictly spatial restrictions.
/// QsRestrictors can come from user-specification:
/// ... WHERE qserv_areaspec_box(1,1,2,2) ...
/// but may be auto-detected from predicates in the where clause.
/// ... WHERE objectId IN (1,2,3,4) ... --> qserv_objectid(1,2,3,4)
/// Some metadata checking is done in the process.
/// Names are generally one of:
/// qserv_fct_name :
///   "qserv_areaspec_box"^
///    | "qserv_areaspec_circle"^
///    | "qserv_areaspec_ellipse"^
///    | "qserv_areaspec_poly"^
///    | "qserv_areaspec_hull"^
/// but may include other names.
class QsRestrictor {
public:

    QsRestrictor() = default;

    typedef std::shared_ptr<QsRestrictor> Ptr;
    typedef std::vector<Ptr> PtrVector;

    bool operator==(const QsRestrictor& rhs) const;

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    virtual void renderTo(QueryTemplate& qt) const = 0;

    /**
     * @brief Serialize to the given ostream for debug output.
     */
    virtual std::ostream& dbgPrint(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os, QsRestrictor const& q);

protected:
    /**
     * @brief Test if this is equal with rhs.
     *
     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    virtual bool isEqual(const QsRestrictor& rhs) const = 0;
};


class QsRestrictorFunction : public QsRestrictor {
public:
    QsRestrictorFunction() = default;

    QsRestrictorFunction(std::string const& name, std::vector<std::string> const& parameters)
            : _name(name), _params(parameters) {}

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    void renderTo(QueryTemplate& qt) const override;

    /**
     * @brief Get the function name.
     *
     * @return std::string const&
     */
    std::string const& getName() const { return _name; }

    /**
     * @brief Get the function parameters.
     *
     * @return StringVector const&
     */
    StringVector const& getParameters() const { return _params; }

    /**
     * @brief Serialize to the given ostream for debug output.
     */
    std::ostream& dbgPrint(std::ostream& os) const override;

protected:
    /**
     * @brief Test if this and rhs are equal.

     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    bool isEqual(const QsRestrictor& rhs) const override;

private:
    std::string const _name;
    StringVector const _params;
};


class SIRestrictor : public QsRestrictor {
public:
    virtual std::shared_ptr<query::ColumnRef const> getSecondaryIndexColumnRef() const = 0;

    virtual std::string getSILookupQuery(std::string const& secondaryIndexDb,
                                         std::string const& secondaryIndexTable,
                                         std::string const& chunkColumn,
                                         std::string const& subChunkColumn) const = 0;

};


class SICompRestrictor : public SIRestrictor {
public:
    SICompRestrictor() = default;

    SICompRestrictor(std::shared_ptr<query::CompPredicate> compPredicate, bool useLeft)
            : _compPredicate(compPredicate), _useLeft(useLeft) {}

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    void renderTo(QueryTemplate& qt) const override;

    /**
     * @brief Serialize to the given ostream for debug output.
     */
    std::ostream& dbgPrint(std::ostream& os) const override;

    std::shared_ptr<query::ColumnRef const> getSecondaryIndexColumnRef() const override;

    std::string getSILookupQuery(std::string const& secondaryIndexDb, std::string const& secondaryIndexTable,
                                 std::string const& chunkColumn, std::string const& subChunkColumn) const override;

    std::shared_ptr<const query::CompPredicate> getCompPredicate() const { return _compPredicate; }

protected:
    /**
     * @brief Test if this and rhs are equal.

     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    bool isEqual(const QsRestrictor& rhs) const override;

private:
    std::shared_ptr<query::CompPredicate> _compPredicate; //< the comparison for this restrictor.
    bool _useLeft; //< true if the secondary index column is on the left of the ComPredicate (false for right)
};


class SIBetweenRestrictor : public SIRestrictor {
public:
    SIBetweenRestrictor() = default;

    SIBetweenRestrictor(std::shared_ptr<query::BetweenPredicate> betweenPredicate)
            : _betweenPredicate(betweenPredicate) {}

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    void renderTo(QueryTemplate& qt) const override;

    /**
     * @brief Serialize to the given ostream for debug output.
     */
    std::ostream& dbgPrint(std::ostream& os) const override;

    std::shared_ptr<query::ColumnRef const> getSecondaryIndexColumnRef() const override;

    std::string getSILookupQuery(std::string const& secondaryIndexDb, std::string const& secondaryIndexTable,
                                 std::string const& chunkColumn, std::string const& subChunkColumn) const override;

protected:
    /**
     * @brief Test if this is equal with rhs.
     *
     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    bool isEqual(const QsRestrictor& rhs) const override;

private:
    // Currently the only place the secondary index column appears is in the `value` parameter of the
    // BetweenPredicate.
    std::shared_ptr<query::BetweenPredicate> _betweenPredicate;
};
}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_QSRESTRICTOR_H
