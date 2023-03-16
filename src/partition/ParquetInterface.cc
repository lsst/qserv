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

// #include "arrow/util/key_value_metadata.h"

#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>

#include "ParquetInterface.h"

namespace lsst::partition {

std::map<std::shared_ptr<arrow::DataType>, int> typeBufSize{
        {arrow::int8(), 3},     {arrow::int16(), 5},    {arrow::int32(), 10},   {arrow::int64(), 20},
        {arrow::uint8(), 3},    {arrow::uint16(), 5},   {arrow::uint32(), 10},  {arrow::uint64(), 20},
        {arrow::boolean(), 1},  {arrow::float16(), 20}, {arrow::float32(), 20}, {arrow::float64(), 20},
        {arrow::float16(), 20}, {arrow::date32(), 20},  {arrow::date64(), 20}};

ParquetFile::ParquetFile(std::string fileName, int maxMemAllocated)
        : m_path_to_file(fileName),
          m_maxMemory(maxMemAllocated),
          m_vmRSS_init(0),
          m_batchNumber(0),
          m_batchSize(0) {}

// Memory used by the current process
int ParquetFile::DumpProcessMemory(std::string idValue, bool bVerbose) const {
    int tSize = 0, resident = 0, share = 0;
    std::ifstream buffer("/proc/self/statm");
    buffer >> tSize >> resident >> share;
    buffer.close();

    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;  // in case x86-64 is configured to use 2MB pages

    double vmSize = (tSize * page_size_kb) / 1024.0;
    double rss = (resident * page_size_kb) / 1024.0;
    double shared_mem = (share * page_size_kb) / 1024.0;

    if (bVerbose) {
        std::cout << "VmSize - " << vmSize << " MB  ";
        std::cout << "VmRSS - " << rss << " MB  ";
        std::cout << "Shared Memory - " << shared_mem << " MB  ";
        std::cout << "Private Memory - " << rss - shared_mem << "MB" << std::endl;
    }

    if (!idValue.empty()) {
        std::map<std::string, int> res{{"VmSize", vmSize}, {"VmRSS", rss}, {"SharedMem", shared_mem}};
        if (res.find(idValue) != res.end()) return res[idValue];
    }
    return 0;
}

// Compute the memory size of a row in butes by adding its element size
//   stringDefaultSize is the default size of a parameter identified as a string
int ParquetFile::GetRecordSize(std::shared_ptr<arrow::Schema> schema, int stringDefaultSize) const {
    int recordSize = 0;

    const arrow::FieldVector& vFields = schema->fields();
    for (const auto& field : vFields) {
        int fieldSize = field->type()->byte_width();
        if (fieldSize < 0) fieldSize = stringDefaultSize;
        recordSize += fieldSize;
    }
    std::cout << "Record size (Bytes) " << recordSize << std::endl;
    return recordSize;
}

// Compute the memory size of a row in butes by adding its element size
//   stringDefaultSize is the default size of a parameter identified as a string
int ParquetFile::GetStringRecordSize(std::shared_ptr<arrow::Schema> schema, int stringDefaultSize) const {
    int recordSize = 0;

    typeBufSize.insert({arrow::utf8(), stringDefaultSize});
    typeBufSize.insert({arrow::large_utf8(), stringDefaultSize});

    const arrow::FieldVector& vFields = schema->fields();
    for (const auto& field : vFields) {
        int fieldSize = typeBufSize[field->type()];
        recordSize += fieldSize;
        recordSize++;
    }
    std::cout << "Record size (approx. CSV string length)  " << recordSize << std::endl;
    return recordSize;
}

// setup the reader that access te parquet file
arrow::Status ParquetFile::SetupBatchReader(int maxBufferSize) {
    m_vmRSS_init = DumpProcessMemory("VmRSS", true);

    int fileRowNumber = GetTotalRowNumber(m_path_to_file);

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    m_batchSize = 5000;                              // batchSize is in fact the number of rows
    arrow_reader_props.set_batch_size(m_batchSize);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.OpenFile(m_path_to_file, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    ARROW_ASSIGN_OR_RAISE(m_arrow_reader_gbl, reader_builder.Build());
    ARROW_RETURN_NOT_OK(m_arrow_reader_gbl->GetRecordBatchReader(&m_rb_reader_gbl));

    // Compute the nimber of lines read by each batch in function of the maximum memory
    //     allocated to the process
    std::shared_ptr<::arrow::Schema> schema;
    arrow::Status st = m_arrow_reader_gbl->GetSchema(&schema);

    // std::cout<<schema->ToString()<<std::endl;
    m_recordSize = GetRecordSize(schema);
    double tmp = double(m_maxMemory) * 1024 * 1024 * 0.85;
    std::cout << "Batch size mem " << tmp << std::endl;
    int64_t batchSize_mem = int64_t(tmp / m_recordSize);  // .85 is a "a la louche" factor
    std::cout << "Max RAM (MB): " << m_maxMemory << "  //  record size : " << m_recordSize
              << "   -> batch size : " << batchSize_mem << std::endl;

    int64_t batchSize_buf = -1;
    m_maxBufferSize = maxBufferSize;
    if (maxBufferSize > 0) {
        m_recordBufferSize = GetStringRecordSize(schema);
        // batchSize_buf = int((maxBufferSize*1024*1024)/m_recordBufferSize);
        batchSize_buf = int(maxBufferSize / m_recordBufferSize);
        std::cout << "\nMax buffer size : " << maxBufferSize << " vs " << m_recordBufferSize
                  << "  -> batch size : " << batchSize_buf << std::endl;
    }

    m_batchSize = std::min(batchSize_mem, batchSize_buf);
    m_arrow_reader_gbl->set_batch_size(m_batchSize);
    m_totalBatchNumber = int(fileRowNumber / m_batchSize);
    if (m_totalBatchNumber * m_batchSize < fileRowNumber) m_totalBatchNumber++;

    std::cout << "Number of rows : " << fileRowNumber << "  batchSize " << m_batchSize << std::endl;
    std::cout << "RecordBatchReader : batch number " << m_totalBatchNumber << std::endl;
    return arrow::Status::OK();
}

int ParquetFile::GetTotalRowNumber(std::string fileName) const {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(fileName, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<parquet::FileMetaData> metadata = reader->parquet_reader()->metadata();
    // std::cout<<"Metadata version : "<<metadata->version()<<std::endl;
    // std::cout<<"Metadata created : "<<metadata->created_by() <<std::endl;
    // std::cout<<"Metadata #row groups : "<<metadata->num_row_groups() <<std::endl;
    // std::cout<<"Metadata #rows : "<<metadata->num_rows() <<std::endl;

    return metadata->num_rows();
}

// Read an arrow batch, format the table acording to the config file and save it in csv format
arrow::Status ParquetFile::ReadNextBatch_Table2CSV(void* buf, int& buffSize,
                                                   std::vector<std::string> params) {
    std::shared_ptr<arrow::Table> table_loc;

    m_parameterNames = params;
    arrow::Status batchStatus = ReadNextBatchTable_Formatted(table_loc);

    if (!batchStatus.ok()) return arrow::Status::ExecutionError("Error while reading and formating batch");

    arrow::Status status = Table2CSVBuffer(table_loc, buffSize, buf);

    if (status.ok()) return arrow::Status::OK();

    return arrow::Status::ExecutionError("Error while writing table to CSV buffer");
}

// Create char buffer containing the table in csv format
arrow::Status ParquetFile::Table2CSVBuffer(std::shared_ptr<arrow::Table>& table, int& buffSize, void* buf) {
    ARROW_ASSIGN_OR_RAISE(auto outstream, arrow::io::BufferOutputStream::Create(1 << 10));

    // Options : null string, no header, no quotes around strings
    arrow::csv::WriteOptions writeOpt = arrow::csv::WriteOptions::Defaults();
    writeOpt.null_string = "\\N";
    writeOpt.include_header = false;
    writeOpt.quoting_style = arrow::csv::QuotingStyle::None;

    ARROW_RETURN_NOT_OK(arrow::csv::WriteCSV(*table, writeOpt, outstream.get()));
    ARROW_ASSIGN_OR_RAISE(auto buffer, outstream->Finish());

    // auto buffer_ptr = buffer.get()->data();
    buffSize = buffer->size();
    std::cout << "ParquetFile::Table2CSVBuffer - buffer length : " << buffSize << " // " << m_maxBufferSize
              << std::endl;

    memcpy(buf, (void*)buffer.get()->data(), buffer->size());

    //  auto myfile = std::fstream("buffer_"+std::to_string(m_batchNumber)+".csv", std::ios::out);
    //  myfile.write((char*)&buffer_ptr[0], buffSize);
    //  myfile.close();

    return arrow::Status::OK();
}

// Read a batch from the file and format the table iaccording to the partition configuration file
//    -> column reordering, true/false -> 1/0, remove null values, ...
arrow::Status ParquetFile::ReadNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& outputTable) {
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch = m_rb_reader_gbl->Next();

    std::vector<std::string> paramNotFound;
    std::map<std::string, std::shared_ptr<arrow::Field>> fieldConfig;

    if (maybe_batch != nullptr) {
        // DumpProcessMemory("VmRSS", true);

        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        std::shared_ptr<arrow::Table> initTable;
        ARROW_ASSIGN_OR_RAISE(initTable, arrow::Table::FromRecordBatches(batch->schema(), {batch}));

        // Increment the batch number
        m_batchNumber++;

        const arrow::FieldVector fields = initTable->schema()->fields();
        for (auto fd : fields) {
            fieldConfig[fd->name()] = fd;
        }

        arrow::FieldVector formatedTable_fields;
        std::vector<std::shared_ptr<arrow::ChunkedArray>> formatedTable_columns;

        // Loop over the column names as defined in the partition config file
        for (std::string paramName : m_parameterNames) {
            std::shared_ptr<arrow::ChunkedArray> chunkedArray = initTable->GetColumnByName(paramName);

            // Column not found in the arrow table...
            if (chunkedArray == NULLPTR) {
                paramNotFound.push_back(paramName);
            } else {
                // Column type is boolean -> switch to 0/1 representation
                if (fieldConfig[paramName]->type() == arrow::boolean()) {
                    auto newChunkedArray = ChunkArrayReformatBoolean(chunkedArray, true);
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
                std::cout << "ERROR : param name " << name << " not found in table columns" << std::endl;
            return arrow::Status::ExecutionError("Configuration file : missing parameter in table");
        }

        // Create the arrow::schema of the new table
        std::shared_ptr<arrow::Schema> formatedSchema = std::make_shared<arrow::Schema>(
                arrow::Schema(formatedTable_fields, initTable->schema()->endianness()));

        // and finally create the arrow::Table that matches the partition config file
        outputTable = arrow::Table::Make(formatedSchema, formatedTable_columns);
        arrow::Status resTable = outputTable->ValidateFull();
        if (!resTable.ok()) {
            std::cout << "ERROR : formated table full validation not OK" << std::endl;
            return arrow::Status::ExecutionError("CSV output table not valid");
        }

        return arrow::Status::OK();
    }

    // The end of the parquet file has been reached
    return arrow::Status::ExecutionError("End of RecorBatchReader iterator");
}

// Reformat a boolean chunk array : true/false boolean array => 1/0 int8 array
std::shared_ptr<arrow::ChunkedArray> ParquetFile::ChunkArrayReformatBoolean(
        std::shared_ptr<arrow::ChunkedArray>& inputArray, bool bCheck) {
    std::vector<std::shared_ptr<arrow::Array>> newChunks;
    std::shared_ptr<arrow::Array> array;
    arrow::Int8Builder builder;

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
            std::cout << errorMsg << std::endl;
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

    auto newChunkedArray = std::make_shared<arrow::ChunkedArray>(std::move(newChunks));

    auto status = newChunkedArray->ValidateFull();
    if (!status.ok()) {
        std::string errorMsg = "Invalid new chunkArraay :  " + status.ToString();
        std::cout << errorMsg << std::endl;
        return nullptr;
    }

    return newChunkedArray;
}

}  // namespace lsst::partition
