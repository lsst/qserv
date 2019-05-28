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


#ifndef LSST_QSERV_QUERY_VALUEEXPR_H
#define LSST_QSERV_QUERY_VALUEEXPR_H


// System headers
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

// Local headers
#include "query/ColumnRef.h"
#include "query/typedefs.h"
#include "util/PointerCompare.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
    class TableRef;
    class ValueFactor;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// ValueExpr is a general value expression in a SQL statement. It is allowed to
/// have an alias and a single level of ValueFactors joined by arithmetic
/// operators. No nesting is allowed yet.
class ValueExpr {
public:
    // in Op: DIVIDE is the `/` operator, "Division; quotient of operands" as specified by MySQL.
    //        DIV is the `DIV` operator, "Division; integer quotient of operands" as specified by MySQL.
    //        THE BIT_ values are bitwise operators: BIT_SHIFT_LEFT is <<, BIT_SHIFT_RIGHT is >>,
    //        BIT_AND is &, BIT_OR is |, BIT_XOR is ^.
    enum Op {NONE=200, UNKNOWN, PLUS, MINUS, MULTIPLY, DIVIDE, DIV, MOD, MODULO,
        BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_AND, BIT_OR, BIT_XOR};
    struct FactorOp {
        explicit FactorOp(std::shared_ptr<ValueFactor> factor_, Op op_=NONE)
            : factor(factor_), op(op_) {}
        FactorOp() : op(NONE) {}
        std::shared_ptr<ValueFactor> factor;
        Op op;
        bool operator==(const FactorOp& rhs) const;
        bool isSubsetOf(FactorOp const& rhs) const;
    };
    typedef std::vector<FactorOp> FactorOpVector;
    friend std::ostream& operator<<(std::ostream& os, FactorOp const& fo);

    ValueExpr();
    ValueExpr(FactorOpVector factorOpVec);

    void addValueFactor(std::shared_ptr<query::ValueFactor> valueFactor);

    bool addOp(query::ValueExpr::Op op);

    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& alias);
    bool hasAlias() const { return not _alias.empty(); }

    /**
     * @brief Set a flag to indicate if the alias was defined by the user in the select statement.
     *
     * For example 'SELECT object AS o' as the user-defined alias 'o'. Otherwise internally Qserv may assign
     * an alias for disambiguation, e.g. in the results table, but that alias should not be used in the
     * select statement used to return results to the user.
     *
     * @param isUserDefined true if the alias was defined by the user.
     */
    void setAliasIsUserDefined(bool isUserDefined) { _aliasIsUserDefined = isUserDefined; }

    // get if the alias is user defined
    bool getAliasIsUserDefined() const { return _aliasIsUserDefined; }

    /// @return a list of ValueFactor-Op
    FactorOpVector& getFactorOps() { return _factorOps; }
    /// @return a const list of ValueFactor-Op
    FactorOpVector const& getFactorOps() const { return _factorOps; }
    /// @return a reference to the list of ValueFactor-Op
    /// this allows unit tests to make modifications
    FactorOpVector& getFactorOpsRef() { return _factorOps; }

    std::shared_ptr<ColumnRef> copyAsColumnRef() const;

    std::string copyAsLiteral() const;

    /// @return true if this contains exactly one FactorOp and its `isConstVal` returns true.
    bool isConstVal() const;

    template<typename T>
    T copyAsType(T const& defaultValue) const;

    void findColumnRefs(ColumnRef::Vector& vector) const;

    /*
     * Check if at least one of the FactorOps of the
     * ValueExpr contains an aggregation function call
     *
     * @return boolean
     */
    bool hasAggregation() const;

    /**
     * @return The ColumnRef in current object if there is exactly one factor and it is a ColumnRef factor,
     *         otherwise returns nullptr.
     */
    ColumnRef::Ptr getColumnRef() const;

    /**
     * @return the first value factor if there are any.
     *
     * @throws logic_error if there are no value factors.
     */
    std::shared_ptr<ValueFactor const> getFactor() const;
    std::shared_ptr<ValueFactor> getFactor();

    /**
     * @brief Check if this ValueExpr represents a star factor.
     *
     * @return true if there is exactly 1 factor, and it is a STAR factor.
     */
    bool isStar() const;

    /**
     * @brief Check if this ValueExpr represents a column ref factor.
     *
     * @return true if there is exactly 1 factor, and it is a COLUMNREF factor.
     */
    bool isColumnRef() const;

    /**
     * @brief Check if this ValueExpr represents a function factor.
     *
     * @return true if there is exactly 1 factor, and it is a FUNCTION factor.
     */
    bool isFunction() const;

    /**
     * @brief Check if this ValueExpr represents a single factor.
     *
     * @return Returns true if there is exactly 1 factor.
     */
    bool isFactor() const;

    /**
     * @brief Get the sql string that this ValueExpr represents
     *
     * @param aliasOnly if this ValueExpr has an alias and this is true then only the alias
     * @return std::string
     */
    std::string sqlFragment(bool preferAlias) const;

    ValueExprPtr clone() const;
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* ve);

    static ValueExprPtr newSimple(std::shared_ptr<ValueFactor> vt);
    static ValueExprPtr newColumnExpr(std::string const& db, std::string const& table,
                                      std::string const& alias, std::string const& column);

    class render;
    friend class render;

    bool operator==(const ValueExpr& rhs) const;

    // compare with another ValueExpr but ignore the alias.
    bool compareValue(const ValueExpr& rhs) const;

    // determine if this object is the same as or a less complete description of the passed in object.
    bool isSubsetOf(ValueExpr const& valueExpr) const;

private:
    std::string _alias;
    FactorOpVector _factorOps;
    bool _aliasIsUserDefined; /// true if the alias was defined by the user in the select statement.
};


/// A helper functor for rendering to QueryTemplates
class ValueExpr::render {
public:
    render(QueryTemplate& qt, bool needsComma, bool isProtected=false)
        : _qt(qt),
          _needsComma(needsComma),
          _isProtected(isProtected),
          _count(0) {}
    void applyToQT(ValueExpr const& ve);
    void applyToQT(ValueExpr const* vep) {
        if(vep) applyToQT(*vep); }
    void applyToQT(std::shared_ptr<ValueExpr> const& vep) {
        applyToQT(vep.get()); }
    QueryTemplate& _qt;
    bool _needsComma;
    bool _isProtected;
    int _count;
};


void cloneValueExprPtrVector(ValueExprPtrVector& dest,
                             ValueExprPtrVector const& src);


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_VALUEEXPR_H
