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

/// \file
/// \brief read parquet file using libarrow library (RecordBatchReader interface)

// System headers
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <unistd.h>

// Third party headers
#include "arrow/api.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/chunked_array.h"
#include "arrow/io/api.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

namespace lsst::partition {

/**
 * Class ParquetFile is used to read a parquet file using the parquet feature defined in libarrow.
 * This method has been developed in order to read the parquet file batch by batch (a batch is a group
 * of rows) and therefore to control the amount of RAM used by the process.
 * Indeed, to this day (May 2023), the parquet files are created in one block without using the rowgroup
 * option.
 *
 * The main idea is to read the file by group of rows, each arrow table build from this rows is then formated
 * - true/false boolean are replaced by 1/0
 * - arrow::csv::WriteCSV function is used to convert the formated table to a CSV character buffer
 * - null values are replaced by the null charcater defined in csv.in.null
 * the CSV buffer is then returned to the partitioner.
 *
 * This method is not MT compliant.
 */
class ParquetFile {
public:
    /**
     * Parquet file constructor
     * @param fileName - parquet file name
     * @param maxMemAllocatedMB - max RAM allocated to the process
     */
    ParquetFile(std::string fileName, int maxMemAllocatedMB = 3000);

    // Disable copy construction and assignment.
    ParquetFile(ParquetFile const&) = delete;
    ParquetFile& operator=(ParquetFile const&) = delete;

    ~ParquetFile();

    /**
     * This method initializes the arrow batch reader. The number of data rows read by each batch is defined
     * to match the constraints defined by the maximum RAM allocate to the reading process and the maximum
     * buffer size as defined in the partitioner configration file
     * @param maxBufferSize - maximum buffer size as defined in the partitioner configuration file
     * @throws arrow::raise_error if the arrow parquet interface or the batch reader cannot be be setup
     */
    void setupBatchReader(int maxBufferSize = -1);

    // These counters return general parameters of the file. Values of the parameters
    // are available after calling method setupBatchReader().

    std::int64_t getFileSize() const { return _fileSize; }
    int getNumRowGroups() const { return _numRowGroups; }
    int getNumRowsTotal() const { return _numRowsTotal; }

    /**
     * This method reads an arrow batch, formats the table acording to the partition configuration file and
     * saves it in csv format
     * @param buf - character buffer containing the content of the arrow table dumped in CSV format
     * @param buffSize - CSV buffer size returned by the function
     * @param columns - names of the data columns to be retrieved as defined in the partitioner configuration
     * file
     * @param optionalColumns - optional parameters as defined in the partitioner configuration file
     * @param nullString - string that replaces a null value in the csv output buffer
     * @param delimStr - delimiter used between values in csv buffer
     * @param quote - if true the double quote fields in the CSV buffer
     * @returns  true if the batch has been read and formated, false if the end of the file has been reached
     * @throws arrow::raise_error if batch could not be read or if the table formating process goes wrong
     */
    bool readNextBatch_Table2CSV(void* buf, int& buffSize, std::vector<std::string> const& columns,
                                 std::set<std::string> const& optionalColumns, std::string const& nullStr,
                                 std::string const& delimStr, bool quote);

    int getBatchSize() const { return _batchSize; }
    int getTotalBatchNumber() const { return _totalBatchNumber; }

private:
    /**
     * This method monitores the memory used by the current procees
     * @param idValue - memory type to monitor (VmSize, VmRSS or SharedMem)
     * @param bVerbose - verbosity level
     * @returns the memory (MB) used by the process
     */
    int _dumpProcessMemory(std::string idValue = "", bool bVerbose = false) const;

    /**
     * This method computates the size in bytes of a parquet data row
     * @param schema - arrow table schema (list of the column names and types )
     * @param stringDefaultSize - the default size assigned to a column with string type
     * @returns the size in bytes of a parquet data row
     */
    int _getRecordSize(std::shared_ptr<arrow::Schema> schema, int defaultSize = 32) const;

    /**
     * This method computates an approximation of the size of a CSV row corresponding to a parquet data row
     * @param schema - arrow table schema (list of the column names and types )
     * @param stringDefaultSize - the default size assigned to a column with string type
     * @returns an approximation if a CSV string corresponding to a parquet data row
     */
    int _getStringRecordSize(std::shared_ptr<arrow::Schema> schema, int defaultSize = 32) const;

    /**
     * This method read the next arrow batch data and proceed to some data formating (column reodering as
     * defined by partitioner, true/false -> 0/1).
     * @param outputTable - arrow table containing the data read by the arrow:batch
     * @returns  true if the batch has been read, false if the end of the file has been reached
     * @throws arrow::raise_error if batch could not be read, if the data table could not be read from the
     * batch, if a data column needed by the partitioner is not found in the table or if the outptTable is not
     * valid as defined
     */
    bool _readNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& table);

    /**
     * This method creates a character buffer containing the input arrow::Table data. CSV conversion is done
     * using the arrow::csv functionality.
     * @param table - arrow table containing the data to be dumped in the CSV buffer
     * @param buffSize - CSV buffer size returned by the function
     * @param buf - character buffer containing the content of the arrow table dumped in CSV format
     * @param nullStr - string that replaces a null value in the csv output buffer
     * @param delimStr - delimiter used betweenn values in csv buffer
     * @param quote - if true the double quote fields in the CSV buffer
     * @throws arrow::raise_error if CSV conversion could not be done
     */
    void _table2CSVBuffer(std::shared_ptr<arrow::Table> const& table, int& buffSize, void* buf,
                          std::string const& nullStr, std::string const& delimStr, bool quote);

    /**
     * This method extract counters from the metadata section of the input file.
     * @throws parquet::throw_error if file could not be open or parquet reader could not be defined
     */
    void _getTotals();

    /**
     * This method reformates a boolean chunk array : a true/false boolean array becomes a 1/0 int8 array
     * @param inputArray - chunkedArray to reformat
     * @param bCheck - boolean to check or not if the formating went right
     * @returns  the formated chunkedArray
     * @throws arrow::raise_error if batch could not be read, if the data table could not be read from the
     * batch, if a data column needed by the partitioner is not found in the table or if the outptTable is not
     * valid as defined
     */
    std::shared_ptr<arrow::ChunkedArray> _chunkArrayReformatBoolean(
            std::shared_ptr<arrow::ChunkedArray>& inputArray, bool bCheck = false);

    std::string _path_to_file;

    int _maxMemoryMB;
    int _recordSize;
    int _recordBufferSize;
    int _vmRSS_init;
    int _batchSize;
    int _totalBatchNumber;
    int _maxBufferSize;
    int64_t _fileSize = 0;
    int _numRowGroups = 0;
    int _numRowsTotal = 0;

    std::vector<std::string> _columns;
    std::set<std::string> _optionalColumns;
    std::unique_ptr<parquet::arrow::FileReader> _arrow_reader_gbl;
    std::unique_ptr<::arrow::RecordBatchReader> _rb_reader_gbl;
};
}  // namespace lsst::partition
