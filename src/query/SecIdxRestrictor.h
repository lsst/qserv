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

#ifndef LSST_QSERV_QUERY_SECIDXRESTRICTOR_H
#define LSST_QSERV_QUERY_SECIDXRESTRICTOR_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Forward declarations
namespace lsst::qserv::query {
class BetweenPredicate;
class ColumnRef;
class CompPredicate;
class InPredicate;
class QueryTemplate;
}  // namespace lsst::qserv::query

namespace lsst::qserv::query {

/// SecIdxRestrictor is a Qserv spatial restrictor element that is used to signal dependencies on
/// spatially-partitioned tables that make use of the secondary index.

class SecIdxRestrictor {
public:
    SecIdxRestrictor() = default;
    virtual ~SecIdxRestrictor() = default;

    bool operator==(const SecIdxRestrictor& rhs) const;

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    virtual void renderTo(QueryTemplate& qt) const = 0;

    friend std::ostream& operator<<(std::ostream& os, SecIdxRestrictor const& q);

    virtual std::shared_ptr<query::ColumnRef const> getSecIdxColumnRef() const = 0;

    virtual std::string getSecIdxLookupQuery(std::string const& secondaryIndexDb,
                                             std::string const& secondaryIndexTable,
                                             std::string const& chunkColumn,
                                             std::string const& subChunkColumn) const = 0;

    /**
     * @brief Get the sql string that this AreaRestrictor represents
     *
     * @return std::string
     */
    std::string sqlFragment() const;

protected:
    /**
     * @brief Test if this is equal with rhs.
     *
     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    virtual bool isEqual(const SecIdxRestrictor& rhs) const = 0;
};

class SecIdxCompRestrictor : public SecIdxRestrictor {
public:
    SecIdxCompRestrictor() = default;

    SecIdxCompRestrictor(std::shared_ptr<query::CompPredicate> compPredicate, bool useLeft)
            : _compPredicate(compPredicate), _useLeft(useLeft) {}

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    void renderTo(QueryTemplate& qt) const override;

    std::shared_ptr<query::ColumnRef const> getSecIdxColumnRef() const override;

    std::string getSecIdxLookupQuery(std::string const& secondaryIndexDb,
                                     std::string const& secondaryIndexTable, std::string const& chunkColumn,
                                     std::string const& subChunkColumn) const override;

    std::shared_ptr<const query::CompPredicate> getCompPredicate() const { return _compPredicate; }

protected:
    /**
     * @brief Test if this and rhs are equal.

     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    bool isEqual(const SecIdxRestrictor& rhs) const override;

private:
    std::shared_ptr<query::CompPredicate> _compPredicate;  //< the comparison for this restrictor.
    bool _useLeft;  //< true if the secondary index column is on the left of the ComPredicate (false for
                    //right)
};

class SecIdxBetweenRestrictor : public SecIdxRestrictor {
public:
    SecIdxBetweenRestrictor() = default;

    SecIdxBetweenRestrictor(std::shared_ptr<query::BetweenPredicate> betweenPredicate)
            : _betweenPredicate(betweenPredicate) {}

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    void renderTo(QueryTemplate& qt) const override;

    std::shared_ptr<query::ColumnRef const> getSecIdxColumnRef() const override;

    std::string getSecIdxLookupQuery(std::string const& secondaryIndexDb,
                                     std::string const& secondaryIndexTable, std::string const& chunkColumn,
                                     std::string const& subChunkColumn) const override;

protected:
    /**
     * @brief Test if this is equal with rhs.
     *
     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    bool isEqual(const SecIdxRestrictor& rhs) const override;

private:
    // Currently the only place the secondary index column appears is in the `value` parameter of the
    // BetweenPredicate.
    std::shared_ptr<query::BetweenPredicate> _betweenPredicate;
};

class SecIdxInRestrictor : public SecIdxRestrictor {
public:
    SecIdxInRestrictor() = default;

    SecIdxInRestrictor(std::shared_ptr<query::InPredicate> inPredicate) : _inPredicate(inPredicate) {}

    /**
     * @brief Serialze this instance as SQL to the QueryTemplate.
     */
    void renderTo(QueryTemplate& qt) const override;

    std::shared_ptr<query::ColumnRef const> getSecIdxColumnRef() const override;

    std::string getSecIdxLookupQuery(std::string const& secondaryIndexDb,
                                     std::string const& secondaryIndexTable, std::string const& chunkColumn,
                                     std::string const& subChunkColumn) const override;

protected:
    /**
     * @brief Test if this and rhs are equal.

     * This is an overidable helper function for operator==, it should only be called by that function, or at
     * least make sure that typeid(this) == typeid(rhs) before calling isEqual.
     */
    bool isEqual(const SecIdxRestrictor& rhs) const override;

private:
    std::shared_ptr<query::InPredicate> _inPredicate;  //< the comparison for this restrictor.
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_SECIDXRESTRICTOR_H
