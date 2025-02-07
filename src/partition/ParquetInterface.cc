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

// Third party headers
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.partitioner");
}  // namespace

namespace lsst::partition {

std::map<std::shared_ptr<arrow::DataType>, int> typeBufSize{
        {arrow::int8(), 3},     {arrow::int16(), 5},    {arrow::int32(), 10},   {arrow::int64(), 20},
        {arrow::uint8(), 3},    {arrow::uint16(), 5},   {arrow::uint32(), 10},  {arrow::uint64(), 20},
        {arrow::boolean(), 1},  {arrow::float16(), 20}, {arrow::float32(), 20}, {arrow::float64(), 20},
        {arrow::float16(), 20}, {arrow::date32(), 20},  {arrow::date64(), 20}};

ParquetFile::ParquetFile(std::string fileName, int maxMemAllocated)
        : _path_to_file(fileName),
          _maxMemory(maxMemAllocated),
          _vmRSS_init(0),
          _batchNumber(0),
          _batchSize(0) {
    LOGS(_log, LOG_LVL_DEBUG, "Partitioner parquet interface...");
}

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
        LOGS(_log, LOG_LVL_DEBUG, "VmSize - " << vmSize << " MB  ");
        LOGS(_log, LOG_LVL_DEBUG, "VmRSS - " << rss << " MB  ");
        LOGS(_log, LOG_LVL_DEBUG, "Shared Memory - " << shared_mem << " MB  ");
        LOGS(_log, LOG_LVL_DEBUG, "Private Memory - " << rss - shared_mem << "MB");
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
    LOGS(_log, LOG_LVL_DEBUG, "Record size (Bytes) " << recordSize);
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
    LOGS(_log, LOG_LVL_DEBUG, "Record size (approx. CSV string length)  " << recordSize);
    return recordSize;
}

arrow::Status ParquetFile::setupBatchReader(int maxBufferSize) {
    _vmRSS_init = _dumpProcessMemory("VmRSS", true);

    int fileRowNumber = _getTotalRowNumber(_path_to_file);

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
    ARROW_RETURN_NOT_OK(reader_builder.OpenFile(_path_to_file, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    ARROW_ASSIGN_OR_RAISE(_arrow_reader_gbl, reader_builder.Build());
    ARROW_ASSIGN_OR_RAISE(_rb_reader_gbl, _arrow_reader_gbl->GetRecordBatchReader());

    // Compute the nimber of lines read by each batch in function of the maximum memory
    //     allocated to the process
    std::shared_ptr<::arrow::Schema> schema;
    arrow::Status st = _arrow_reader_gbl->GetSchema(&schema);

    _recordSize = _getRecordSize(schema);
    double tmp = double(_maxMemory) * 1024 * 1024 * 0.85;
    LOGS(_log, LOG_LVL_DEBUG, "Batch size mem " << tmp);
    int64_t batchSize_mem = int64_t(tmp / _recordSize);  // .85 is a "a la louche" factor
    LOGS(_log, LOG_LVL_DEBUG,
         "Max RAM (MB): " << _maxMemory << "  //  record size : " << _recordSize
                          << "   -> batch size : " << batchSize_mem);

    int64_t batchSize_buf = -1;
    _maxBufferSize = maxBufferSize;
    if (maxBufferSize > 0) {
        _recordBufferSize = _getStringRecordSize(schema);
        // batchSize_buf = int((maxBufferSize*1024*1024)/_recordBufferSize);
        batchSize_buf = int(maxBufferSize / _recordBufferSize);
        LOGS(_log, LOG_LVL_DEBUG,
             "\nMax buffer size : " << maxBufferSize << " vs " << _recordBufferSize
                                    << "  -> batch size : " << batchSize_buf);
    }

    _batchSize = std::min(batchSize_mem, batchSize_buf);
    _arrow_reader_gbl->set_batch_size(_batchSize);
    _totalBatchNumber = int(fileRowNumber / _batchSize);
    if (_totalBatchNumber * _batchSize < fileRowNumber) _totalBatchNumber++;

    LOGS(_log, LOG_LVL_DEBUG, "Number of rows : " << fileRowNumber << "  batchSize " << _batchSize);
    LOGS(_log, LOG_LVL_DEBUG, "RecordBatchReader : batch number " << _totalBatchNumber);
    return arrow::Status::OK();
}

int ParquetFile::_getTotalRowNumber(std::string fileName) const {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(fileName, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    std::shared_ptr<parquet::FileMetaData> metadata = reader->parquet_reader()->metadata();

    return metadata->num_rows();
}

arrow::Status ParquetFile::readNextBatch_Table2CSV(void* buf, int& buffSize,
                                                   std::vector<std::string> const& params,
                                                   std::string const& nullStr, std::string const& delimStr) {
    std::shared_ptr<arrow::Table> table_loc;

    _parameterNames = params;
    // Get the next data batch, data are formated
    arrow::Status batchStatus = _readNextBatchTable_Formatted(table_loc);

    if (!batchStatus.ok()) return arrow::Status::ExecutionError("Error while reading and formating batch");

    arrow::Status status = _table2CSVBuffer(table_loc, buffSize, buf, nullStr, delimStr);

    if (status.ok()) return arrow::Status::OK();

    return arrow::Status::ExecutionError("Error while writing table to CSV buffer");
}

arrow::Status ParquetFile::_table2CSVBuffer(std::shared_ptr<arrow::Table> const& table, int& buffSize,
                                            void* buf, std::string const& nullStr,
                                            std::string const& delimStr) {
    ARROW_ASSIGN_OR_RAISE(auto outstream, arrow::io::BufferOutputStream::Create(1 << 10));

    // Options : null string, no header, no quotes around strings
    arrow::csv::WriteOptions writeOpt = arrow::csv::WriteOptions::Defaults();
    writeOpt.null_string = nullStr;
    writeOpt.delimiter = delimStr[0];
    writeOpt.include_header = false;
    writeOpt.quoting_style = arrow::csv::QuotingStyle::None;

    ARROW_RETURN_NOT_OK(arrow::csv::WriteCSV(*table, writeOpt, outstream.get()));
    ARROW_ASSIGN_OR_RAISE(auto buffer, outstream->Finish());

    // auto buffer_ptr = buffer.get()->data();
    buffSize = buffer->size();
    LOGS(_log, LOG_LVL_DEBUG,
         "ParquetFile::Table2CSVBuffer - buffer length : " << buffSize << " // " << _maxBufferSize);

    memcpy(buf, (void*)buffer.get()->data(), buffer->size());
    return arrow::Status::OK();
}

arrow::Status ParquetFile::_readNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& outputTable) {
    auto const maybe_batch = _rb_reader_gbl->Next();

    std::vector<std::string> paramNotFound;
    std::map<std::string, std::shared_ptr<arrow::Field>> fieldConfig;

    if (maybe_batch != nullptr) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        std::shared_ptr<arrow::Table> initTable;
        ARROW_ASSIGN_OR_RAISE(initTable, arrow::Table::FromRecordBatches(batch->schema(), {batch}));

        // Increment the batch number
        _batchNumber++;

        const arrow::FieldVector fields = initTable->schema()->fields();
        for (auto fd : fields) {
            fieldConfig[fd->name()] = fd;
        }

        arrow::FieldVector formatedTable_fields;
        std::vector<std::shared_ptr<arrow::ChunkedArray>> formatedTable_columns;

        // Loop over the column names as defined in the partition config file
        for (std::string paramName : _parameterNames) {
            std::shared_ptr<arrow::ChunkedArray> chunkedArray = initTable->GetColumnByName(paramName);

            // Column not found in the arrow table...
            if (chunkedArray == nullptr) {
                paramNotFound.push_back(paramName);
            } else {
                // Column type is boolean -> switch to 0/1 representation
                if (fieldConfig[paramName]->type() == arrow::boolean()) {
                    auto newChunkedArray = _chunkArrayReformatBoolean(chunkedArray, true);
                    if (newChunkedArray == nullptr) {
                        return arrow::Status::ExecutionError("Error while formating boolean chunk array");
                    }
                    formatedTable_columns.push_back(newChunkedArray);

                    std::shared_ptr<arrow::Field> newField =
                            std::make_shared<arrow::Field>(fieldConfig[paramName]->name(), arrow::int8());
                    formatedTable_fields.push_back(newField);
                }
                // Simply keep the chunk as it is defined in teh arrow table
                else {
                    formatedTable_columns.push_back(chunkedArray);
                    formatedTable_fields.push_back(fieldConfig[paramName]);
                }
            }
        }  // end of loop over parameters

        // If a column is not found (i.e. a parameter defined in partition.json does not exist in parquet
        // file), throw an error and stop
        if (paramNotFound.size() > 0) {
            for (auto name : paramNotFound)
                LOGS(_log, LOG_LVL_DEBUG, "ERROR : param name " << name << " not found in table columns");
            return arrow::Status::ExecutionError("Configuration file : missing parameter in table");
        }

        // Create the arrow::schema of the new table
        std::shared_ptr<arrow::Schema> formatedSchema = std::make_shared<arrow::Schema>(
                arrow::Schema(formatedTable_fields, initTable->schema()->endianness()));

        // and finally create the arrow::Table that matches the partition config file
        outputTable = arrow::Table::Make(formatedSchema, formatedTable_columns);
        arrow::Status resTable = outputTable->ValidateFull();
        if (!resTable.ok()) {
            LOGS(_log, LOG_LVL_DEBUG, "ERROR : formated table full validation not OK");
            return arrow::Status::ExecutionError("CSV output table not valid");
        }

        return arrow::Status::OK();
    }

    // The end of the parquet file has been reached
    return arrow::Status::ExecutionError("End of RecorBatchReader iterator");
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
            std::string errorMsg = "ERROR  while finalizing " + inputArray->ToString() + " new chunked array";
            LOGS(_log, LOG_LVL_DEBUG, errorMsg);
            return nullptr;
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
        std::string errorMsg = "Invalid new chunkArraay :  " + status.ToString();
        LOGS(_log, LOG_LVL_DEBUG, errorMsg);
        return nullptr;
    }

    return newChunkedArray;
}

}  // namespace lsst::partition
