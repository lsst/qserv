// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Class header
#include "partition/ParquetInterface.h"

// System headers
#include <fstream>
#include <map>
#include <stdexcept>

// Third party headers
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.partitioner");
char const* prefix = "Parquet : ";
}  // namespace

namespace lsst::partition {

std::map<std::shared_ptr<arrow::DataType>, int> typeBufSize{
        {arrow::int8(), 3},     {arrow::int16(), 5},    {arrow::int32(), 10},   {arrow::int64(), 20},
        {arrow::uint8(), 3},    {arrow::uint16(), 5},   {arrow::uint32(), 10},  {arrow::uint64(), 20},
        {arrow::boolean(), 1},  {arrow::float16(), 20}, {arrow::float32(), 20}, {arrow::float64(), 20},
        {arrow::float16(), 20}, {arrow::date32(), 20},  {arrow::date64(), 20}};

ParquetFile::ParquetFile(std::string fileName, int maxMemAllocatedMB)
        : _path_to_file(fileName), _maxMemoryMB(maxMemAllocatedMB), _vmRSS_init(0), _batchSize(0) {
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Created");
}

ParquetFile::~ParquetFile() { LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Destroyed"); }

int ParquetFile::_dumpProcessMemory(std::string idValue, bool bVerbose) const {
    int tSize = 0, resident = 0, share = 0;
    std::ifstream buffer("/proc/self/statm");
    buffer >> tSize >> resident >> share;
    buffer.close();

    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;  // in case x86-64 is configured to use 2MB pages

    double vmSize = (tSize * page_size_kb) / 1024.0;
    double rss = (resident * page_size_kb) / 1024.0;
    double shared_mem = (share * page_size_kb) / 1024.0;

    if (bVerbose) {
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "VmSize [MB] : " << vmSize);
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "VmRSS [MB] : " << rss);
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Shared Memory [MB] : " << shared_mem);
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Private Memory [MB] : " << rss - shared_mem);
    }

    if (!idValue.empty()) {
        std::map<std::string, int> res{{"VmSize", vmSize}, {"VmRSS", rss}, {"SharedMem", shared_mem}};
        if (res.find(idValue) != res.end()) return res[idValue];
    }
    return 0;
}

int ParquetFile::_getRecordSize(std::shared_ptr<arrow::Schema> schema, int stringDefaultSize) const {
    int recordSize = 0;

    const arrow::FieldVector& vFields = schema->fields();
    for (const auto& field : vFields) {
        int fieldSize = field->type()->byte_width();
        if (fieldSize < 0) fieldSize = stringDefaultSize;
        recordSize += fieldSize;
    }
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Record size [Bytes] : " << recordSize);
    return recordSize;
}

int ParquetFile::_getStringRecordSize(std::shared_ptr<arrow::Schema> schema, int stringDefaultSize) const {
    int recordSize = 0;

    typeBufSize.insert({arrow::utf8(), stringDefaultSize});
    typeBufSize.insert({arrow::large_utf8(), stringDefaultSize});

    const arrow::FieldVector& vFields = schema->fields();
    for (const auto& field : vFields) {
        int fieldSize = typeBufSize[field->type()];
        recordSize += fieldSize;
        recordSize++;
    }
    LOGS(_log, LOG_LVL_DEBUG,
         ::prefix << "Record size (approx. CSV string length) [Bytes] :  " << recordSize);
    return recordSize;
}

void ParquetFile::setupBatchReader(int maxBufferSize) {
    _vmRSS_init = _dumpProcessMemory("VmRSS", true);

    _getTotals();

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    _batchSize = 5000;                              // batchSize is in fact the number of rows
    arrow_reader_props.set_batch_size(_batchSize);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.OpenFile(_path_to_file, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    PARQUET_ASSIGN_OR_THROW(_arrow_reader_gbl, reader_builder.Build());
    PARQUET_ASSIGN_OR_THROW(_rb_reader_gbl, _arrow_reader_gbl->GetRecordBatchReader());

    // Compute the nimber of lines read by each batch in function of the maximum memory
    //     allocated to the process
    std::shared_ptr<::arrow::Schema> schema;
    arrow::Status st = _arrow_reader_gbl->GetSchema(&schema);

    _recordSize = _getRecordSize(schema);
    double tmp = double(_maxMemoryMB) * 1024 * 1024 * 0.85;
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Batch size mem [Bytes] : " << tmp);
    int64_t batchSize_mem = int64_t(tmp / _recordSize);  // .85 is a "a la louche" factor
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Max RAM [MB] : " << _maxMemoryMB);
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Record size [Bytes] : " << _recordSize);
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Batch size [Bytes] : " << batchSize_mem);

    int64_t batchSize_buf = -1;
    _maxBufferSize = maxBufferSize;
    if (maxBufferSize > 0) {
        _recordBufferSize = _getStringRecordSize(schema);
        batchSize_buf = int(maxBufferSize / _recordBufferSize);
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Max buffer size [Bytes] : " << maxBufferSize);
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Record buffer size [Bytes] : " << _recordBufferSize);
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Batch buffer size [Bytes] : " << batchSize_buf);
    }

    _batchSize = std::min(batchSize_mem, batchSize_buf);
    _arrow_reader_gbl->set_batch_size(_batchSize);
    _totalBatchNumber = int(_numRowsTotal / _batchSize);
    if (_totalBatchNumber * _batchSize < _numRowsTotal) _totalBatchNumber++;

    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "RecordBatchReader : batchSize [Bytes] : " << _batchSize);
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "RecordBatchReader : batch number : " << _totalBatchNumber);
}

void ParquetFile::_getTotals() {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile,
                            arrow::io::ReadableFile::Open(_path_to_file, arrow::default_memory_pool()));
    _fileSize = infile->GetSize().ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));
    _numRowGroups = reader->num_row_groups();

    std::shared_ptr<parquet::FileMetaData> metadata = reader->parquet_reader()->metadata();
    _numRowsTotal = metadata->num_rows();

    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Total file size [Bytes] : " << _fileSize);
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Number of row groups : " << _numRowGroups);
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Number of rows : " << _numRowsTotal);
}

bool ParquetFile::readNextBatch_Table2CSV(void* buf, int& buffSize, std::vector<std::string> const& columns,
                                          std::set<std::string> const& optionalColumns,
                                          std::string const& nullStr, std::string const& delimStr,
                                          bool quote) {
    std::shared_ptr<arrow::Table> table_loc;

    _columns = columns;
    _optionalColumns = optionalColumns;

    // Get the next data batch (if any is still left), data are formated
    if (_readNextBatchTable_Formatted(table_loc)) {
        _table2CSVBuffer(table_loc, buffSize, buf, nullStr, delimStr, quote);
        return true;
    }
    return false;
}

void ParquetFile::_table2CSVBuffer(std::shared_ptr<arrow::Table> const& table, int& buffSize, void* buf,
                                   std::string const& nullStr, std::string const& delimStr, bool quote) {
    PARQUET_ASSIGN_OR_THROW(auto outstream, arrow::io::BufferOutputStream::Create(1 << 10));

    // Options : null string, no header, no quotes around strings
    arrow::csv::WriteOptions writeOpt = arrow::csv::WriteOptions::Defaults();
    writeOpt.null_string = nullStr;
    writeOpt.delimiter = delimStr[0];
    writeOpt.include_header = false;
    writeOpt.quoting_style = quote ? arrow::csv::QuotingStyle::AllValid : arrow::csv::QuotingStyle::None;

    arrow::Status status = arrow::csv::WriteCSV(*table, writeOpt, outstream.get());
    if (!status.ok()) {
        std::string const msg = "Error while writing table to CSV buffer";
        LOGS(_log, LOG_LVL_ERROR, ::prefix << msg);
        throw std::runtime_error(msg);
    }
    PARQUET_ASSIGN_OR_THROW(auto buffer, outstream->Finish());

    buffSize = buffer->size();
    LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Buffer size [Bytes] : " << buffSize << " of " << _maxBufferSize);

    memcpy(buf, (void*)buffer.get()->data(), buffer->size());
}

bool ParquetFile::_readNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& outputTable) {
    auto const maybe_batch = _rb_reader_gbl->Next();
    if (maybe_batch == nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, ::prefix << "End of file reached");
        return false;
    }
    PARQUET_ASSIGN_OR_THROW(auto batch, maybe_batch);
    std::shared_ptr<arrow::Table> initTable;
    PARQUET_ASSIGN_OR_THROW(initTable, arrow::Table::FromRecordBatches(batch->schema(), {batch}));

    std::map<std::string, std::shared_ptr<arrow::Field>> fieldConfig;
    const arrow::FieldVector fields = initTable->schema()->fields();
    for (auto fd : fields) {
        fieldConfig[fd->name()] = fd;
    }

    arrow::FieldVector formatedTable_fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> formatedTable_columns;
    std::shared_ptr<arrow::ChunkedArray> null_array;

    // Loop over the column names as defined in the partition config file
    for (auto const& column : _columns) {
        std::shared_ptr<arrow::ChunkedArray> chunkedArray = initTable->GetColumnByName(column);
        if (chunkedArray == nullptr) {
            LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Column name : " << column << " not found in the table");
            // Column not found in the arrow table...
            if (_optionalColumns.count(column) == 0) {
                std::string const msg = "Column '" + column + "' not found in the input file";
                LOGS(_log, LOG_LVL_ERROR, ::prefix << msg);
                throw std::runtime_error(msg);
            }
            // Insert a column with all nulls
            if (null_array == nullptr) {
                null_array = std::make_shared<arrow::ChunkedArray>(
                        std::make_shared<arrow::NullArray>(initTable->num_rows()));
            }
            formatedTable_columns.push_back(null_array);
            formatedTable_fields.push_back(std::make_shared<arrow::Field>(column, arrow::null()));
        } else {
            LOGS(_log, LOG_LVL_DEBUG, ::prefix << "Column name : " << column);
            if (fieldConfig[column]->type() == arrow::boolean()) {
                // Column type is boolean -> switch to 0/1 representation
                auto newChunkedArray = _chunkArrayReformatBoolean(chunkedArray, true);
                if (newChunkedArray == nullptr) {
                    std::string const msg = "Error while formating boolean chunk array";
                    LOGS(_log, LOG_LVL_ERROR, ::prefix << msg);
                    throw std::runtime_error(msg);
                }
                formatedTable_columns.push_back(newChunkedArray);
                std::shared_ptr<arrow::Field> newField =
                        std::make_shared<arrow::Field>(fieldConfig[column]->name(), arrow::int8());
                formatedTable_fields.push_back(newField);
            } else {
                // Simply keep the chunk as it is defined in the arrow table
                formatedTable_columns.push_back(chunkedArray);
                formatedTable_fields.push_back(fieldConfig[column]);
            }
        }
    }

    // Create the arrow::schema of the new table
    std::shared_ptr<arrow::Schema> formatedSchema = std::make_shared<arrow::Schema>(
            arrow::Schema(formatedTable_fields, initTable->schema()->endianness()));

    // and finally create the arrow::Table that matches the partition config file
    outputTable = arrow::Table::Make(formatedSchema, formatedTable_columns);
    arrow::Status resTable = outputTable->ValidateFull();
    if (!resTable.ok()) {
        std::string const msg = "Formated table not valid";
        LOGS(_log, LOG_LVL_ERROR, ::prefix << msg);
        throw std::runtime_error(msg);
    }
    return true;
}

std::shared_ptr<arrow::ChunkedArray> ParquetFile::_chunkArrayReformatBoolean(
        std::shared_ptr<arrow::ChunkedArray>& inputArray, bool bCheck) {
    std::vector<std::shared_ptr<arrow::Array>> newChunks;
    std::shared_ptr<arrow::Array> array;
    arrow::Int8Builder builder;

    // Loop over the chunks defined in the chunkedArray
    const arrow::ArrayVector& chunks = inputArray->chunks();
    for (auto& elemChunk : chunks) {
        std::shared_ptr<arrow::ArrayData> chunkData = elemChunk->data();
        builder.Reset();

        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(elemChunk);
        for (int64_t i = 0; i < elemChunk->length(); ++i) {
            bool bIsNull = bool_array->IsNull(i);
            if (bIsNull)
                arrow::Status status = builder.AppendNull();
            else
                arrow::Status status = builder.Append(bool_array->Value(i));
        }

        if (!builder.Finish(&array).ok()) {
            std::string const msg = "Failed to finalize '" + inputArray->ToString() + "' new chunked array";
            LOGS(_log, LOG_LVL_ERROR, ::prefix << msg);
            throw std::runtime_error(msg);
        }

        if (bCheck) {
            assert(array->length() == elemChunk->length());

            auto new_array = std::static_pointer_cast<arrow::Int8Array>(array);
            for (int64_t i = 0; i < elemChunk->length(); ++i) {
                assert(bool_array->IsNull(i) == array->IsNull(i));
                assert((bool_array->Value(i) == true && new_array->Value(i) != 0) ||
                       (bool_array->Value(i) == false && new_array->Value(i) == 0));
            }
        }

        newChunks.push_back(std::move(array));
    }

    // Create new chunkArray based on modified chunks
    auto newChunkedArray = std::make_shared<arrow::ChunkedArray>(std::move(newChunks));

    // arrow validation of the new chunkedArray
    auto status = newChunkedArray->ValidateFull();
    if (!status.ok()) {
        std::string const msg = "Invalid new chunkArraay :  '" + status.ToString() + "'";
        LOGS(_log, LOG_LVL_ERROR, ::prefix << msg);
        throw std::runtime_error(msg);
    }
    return newChunkedArray;
}

}  // namespace lsst::partition
