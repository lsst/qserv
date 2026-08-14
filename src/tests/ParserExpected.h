// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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
 * @brief Helper for normalizing differences between parser backends.
 *
 * PARSER_EXPECTED(hyrise, antlr) selects the appropriate string literal for the active parser.
 *
 *   "SELECT ... " PARSER_EXPECTED("JOIN", "INNER JOIN") " ORDER BY ..."
 *
 *      -> SELECT ... JOIN ORDER BY ...
 *              - or -
 *      -> SELECT ... INNER JOIN ORDER BY ...
 */

#ifndef LSST_QSERV_TESTS_PARSEREXPECTED_H
#define LSST_QSERV_TESTS_PARSEREXPECTED_H

#ifdef QSERV_USE_HYRISE_SQL_PARSER
#define PARSER_EXPECTED(hyrise, antlr) hyrise
#else
#define PARSER_EXPECTED(hyrise, antlr) antlr
#endif

#endif  // LSST_QSERV_TESTS_PARSEREXPECTED_H
