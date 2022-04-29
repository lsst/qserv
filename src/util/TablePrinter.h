/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_TABLEPRINTER_H
#define LSST_QSERV_UTIL_TABLEPRINTER_H

/**
 *  TablePrinter.h declares:
 *
 * class TablePrinter
 *
 * (see individual class documentation for more information)
 */

// System headers
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <vector>

// This header declarations

namespace lsst::qserv::util {

/**
 * Class ColumnTablePrinter is a pretty-printer for column-based tables.
 * A model of this class has the following assumptions:
 *
 * 1) all columns have the same number of rows
 * 2) all values of a column have the same type (if not then it's up to
 *    to a user to turn them into strings)
 * 3) values of a column can be put into a vector before submitting to
 *    the table printer
 *
 * Example:
 * @code
 *
 *   std::vector<int>         ids     = {0,      1,     2,     3,       4};
 *   std::vector<std::string> names   = {"zero", "one", "two", "three", "four"};
 *   std::vector<double>      weights = {2.1,    4.45,  222.,  110.123, -24.098};
 *   std::vector<std::string> notes   = {
 *     "Lorem ipsum dolor sit amet",
 *     "consectetur adipiscing elit",
 *     "sed do eiusmod tempor incididunt ut labore",
 *     "et dolore magna aliqua.",
 *     "Ut enim ad minim veniam..."
 *   };
 *
 *   ColumnTablePrinter table("My Items --", " -- ");
 *
 *   table.addColumn("id",     ids);
 *   table.addColumn("name",   names, ColumnTablePrinter::Alignment::LEFT);
 *   table.addColumn("weight", weights);
 *   table.addColumn("note",   notes, ColumnTablePrinter::Alignment::LEFT);
 *
 *   table.print(std::cout);
 *
 * @code
 *
 * Output:
 * @code
 *
 *   -- My Items --
 *   -- ----+-------+---------+--------------------------------------------
 *   --  id | name  |  weight | note
 *   -- ----+-------+---------+--------------------------------------------
 *   --   0 | zero  |     2.1 | Lorem ipsum dolor sit amet
 *   --   1 | one   |    4.45 | consectetur adipiscing elit
 *   --   2 | two   |     222 | sed do eiusmod tempor incididunt ut labore
 *   --   3 | three | 110.123 | et dolore magna aliqua.
 *   --   4 | four  | -24.098 | Ut enim ad minim veniam...
 *   -- ----+-------+---------+--------------------------------------------
 *
 * @code
 *
 * The printer has additional options which control an appearance of
 * the printed tables. Setting the following options:
 * @code@
 *   bool   topSeparator    = true;
 *   bool   bottomSeparator = true;
 *   size_t pageSize        = 2;
 *   bool   repeatedHeader  = false;
 * @code
 * And passing then to the printing method will produce the following
 * output:
 * @code
 *
 *   -- My Items --
 *   --
 *   --  id | name  |  weight | note
 *   -- ----+-------+---------+--------------------------------------------
 *   --   0 | zero  |     2.1 | Lorem ipsum dolor sit amet
 *   --   1 | one   |    4.45 | consectetur adipiscing elit
 *   --
 *   --  id | name  |  weight | note
 *   -- ----+-------+---------+--------------------------------------------
 *   --   2 | two   |     222 | sed do eiusmod tempor incididunt ut labore
 *   --   3 | three | 110.123 | et dolore magna aliqua.
 *   --
 *   --  id | name  |  weight | note
 *   -- ----+-------+---------+--------------------------------------------
 *   --   4 | four  | -24.098 | Ut enim ad minim veniam...
 *
 * @code
 *
 * @see tColumnTablePrinter::print()
 */
class ColumnTablePrinter {
public:
    /**
     * Type Alignment defines cell values alignment.
     *
     * @note
     *   Column names and the corresponding values will be aligned
     *   the same way.
     */
    enum Alignment { LEFT, RIGHT };

    /**
     * The normal and also the default constructor
     *
     * @param caption
     *   optional table caption (if any) to be printed before the table
     *
     * @param indent
     *   optional indentation before each line of the table
     *
     * @param verticalSeparator
     *   optional flag indicating if the vertical separators should be printed between
     *   the columns. The default value is 'true'
     */
    explicit ColumnTablePrinter(std::string const& caption = std::string(),
                                std::string const& indent = std::string(), bool verticalSeparator = true);

    ColumnTablePrinter(ColumnTablePrinter const&) = default;
    ColumnTablePrinter& operator=(ColumnTablePrinter const&) = default;

    ~ColumnTablePrinter() = default;

    /**
     * Add column's header and the data
     *
     * @param title
     *   the title of the column to be printed once at the table header
     *
     * @param data
     *   the data a the column cells
     *
     * @throws std::invalid_argument
     *   if the number of rows doesn't match the one of the previously stored
     *   columns
     */
    template <class T>
    void addColumn(std::string const& title, std::vector<T> const& data, Alignment align = RIGHT) {
        // Translate values into a collection of strings, then forward
        // the collection to a method which implements the rest of
        // the operation.

        std::vector<std::string> cells;
        for (auto&& v : data) {
            std::ostringstream ss;
            ss << v;
            cells.push_back(ss.str());
        }
        addColumn(title, cells, align);
    }

    /**
     * Add column's header and the data (specialized for [T=std::string]
     *
     * @param title
     *   the title of the column to be printed once at the table header
     *
     * @param data
     *   the data a the column cells
     *
     * @throws std::invalid_argument
     *   if the number of rows doesn't match the one of the previously stored
     *   columns
     */
    void addColumn(std::string const& title, std::vector<std::string> const& data, Alignment align = RIGHT);

    /**
     * Print the table
     *
     * @param os
     *   the output stream
     *
     * @param topSeparator
     *   optional flag which if set to 'true' will result in printing a row
     *   separator on top of the table header
     *
     * @param bottomSeparator
     *   optional flag which if set to 'true' will result in printing a row
     *   separator at the bottom of the table header
     *
     * @param pageSize
     *   optional "page" size if not equal to 0 will result in printing
     *   a separator after the specified number of rows.
     *
     * @param repeatedHeader
     *   optional flag which further refines the table "pagination"
     *   if requested by parameter 'pageSize'. If the flag is set
     *   to 'true' then a full header will get repeated at the beginning
     *   of each page.
     */
    void print(std::ostream& os, bool topSeparator = true, bool bottomSeparator = true, size_t pageSize = 0,
               bool repeatedHeader = false) const;

private:
    /**
     * @return
     *   the width of the column based on a longest string found among the
     *   method's values
     *
     * @param title
     *   the column header
     *
     * @param cells
     *   the vector of cells to be measured
     */
    static size_t _columnWidth(std::string const& title, std::list<std::string> const& cells);

    /**
     * Initialize table rows with empty strings if this is the very first
     * column reported to the printer. Otherwise make sure the number of rows
     * in the input column matches the one for all previously added columns.
     *
     * @param size
     *   the number of rows
     *
     * @throws std::invalid_argument
     *   if the number of rows doesn't match the one of the previously stored
     *   columns
     */
    void _initRows(size_t numRows);

    /**
     *
     * @param title
     *   the column title to be printed in the headers
     *
     * @param cells
     *   values of the column cells to uppend to the corresponding rows
     */
    void _rightUppendCellsToRows(std::string const& title, std::list<std::string> const& cells,
                                 Alignment align);

private:
    /// Optional table caption (if any) to be printed before the table
    std::string const _caption;

    /// Optional indentation (if any) to be printed at each row
    std::string const _indent;

    bool _verticalSeparator;

    /// The standard separator for the table header and its footer
    std::string _separator;

    /// Header
    std::string _header;

    /// Data rows that are ready to be printed
    std::list<std::string> _rows;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_TABLEPRINTER_H
