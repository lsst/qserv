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

// Class header
#include "util/TablePrinter.h"

// System headers
#include <algorithm>
#include <stdexcept>
#include <iomanip>

namespace lsst::qserv::util {

ColumnTablePrinter::ColumnTablePrinter(std::string const& caption, std::string const& indent,
                                       bool verticalSeparator)
        : _caption(caption), _indent(indent), _verticalSeparator(verticalSeparator) {}

void ColumnTablePrinter::addColumn(std::string const& title, std::vector<std::string> const& data,
                                   ColumnTablePrinter::Alignment align) {
    _initRows(data.size());

    // Translate and put values into cells as strings

    std::list<std::string> cells;
    for (auto&& v : data) {
        cells.push_back(v);
    }
    _rightUppendCellsToRows(title, cells, align);
}

void ColumnTablePrinter::print(std::ostream& os, bool topSeparator, bool bottomSeparator, size_t pageSize,
                               bool repeatedHeader) const {
    if (not _caption.empty()) {
        os << _indent << _caption << "\n";
    }
    if (topSeparator) {
        os << _indent << _separator << "\n";
    } else {
        os << _indent << "\n";
    }
    os << _indent << _header << "\n" << _indent << _separator << "\n";

    size_t currentPageRows = 0;
    for (auto&& r : _rows) {
        os << _indent << r << "\n";
        if (pageSize != 0 and pageSize == ++currentPageRows) {
            currentPageRows = 0;
            if (topSeparator) {
                os << _indent << _separator << "\n";
            } else {
                os << _indent << "\n";
            }
            if (repeatedHeader) {
                os << _indent << _header << "\n" << _indent << _separator << "\n";
            }
        }
    }
    if (bottomSeparator) {
        os << _indent << _separator << "\n";
    }
}

size_t ColumnTablePrinter::_columnWidth(std::string const& title, std::list<std::string> const& cells) {
    size_t width = title.size();
    for (auto const& c : cells) {
        width = std::max(width, c.size());
    }
    return width;
}

void ColumnTablePrinter::_initRows(size_t numRows) {
    if (_rows.size() == 0) {
        for (size_t i = 0; i < numRows; ++i) {
            _rows.push_back(std::string());
        }
    } else {
        if (_rows.size() != numRows) {
            throw std::invalid_argument("util::ColumnTablePrinter::_initRows  the number of rows " +
                                        std::to_string(numRows) + " is not the same as " +
                                        std::to_string(_rows.size()) + " for the previously added columns");
        }
    }
}

void ColumnTablePrinter::_rightUppendCellsToRows(std::string const& title,
                                                 std::list<std::string> const& cells,
                                                 ColumnTablePrinter::Alignment align) {
    size_t const width = _columnWidth(title, cells);

    // Extend the separator
    if (_verticalSeparator) {
        _separator += (_separator.empty() ? "" : "+") + std::string(1 + width + 1, '-');
    } else {
        _separator += (_separator.empty() ? " " : "  ") + std::string(width, '-') + " ";
    }

    // Extend the header
    std::ostringstream titleStream;
    titleStream << " " << (_header.empty() ? "" : (_verticalSeparator ? "| " : "  "))
                << (align == Alignment::LEFT ? std::left : std::right) << std::setw(width) << title;
    _header += titleStream.str();

    // Extend rows with the values of the column's cells
    auto itr = cells.cbegin();
    for (auto&& r : _rows) {
        std::ostringstream cellStream;
        cellStream << " " << (r.empty() ? "" : (_verticalSeparator ? "| " : "  "))
                   << (align == Alignment::LEFT ? std::left : std::right) << std::setw(width) << *(itr++);
        r += cellStream.str();
    }
}

}  // namespace lsst::qserv::util
