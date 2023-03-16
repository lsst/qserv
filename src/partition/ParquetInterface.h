
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/chunked_array.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <map>

// class PartitionConfig;

namespace lsst::partition {

class ParquetFile {
public:
    ParquetFile(std::string fileName, int maxMemAllocated = 3000 /*MB*/);
    arrow::Status SetupBatchReader(int maxBufferSize = -1);
    arrow::Status ReadNextBatch_Table2CSV(void* buf, int& buffSize, std::vector<std::string> params);

    int GetBatchSize() const { return m_batchSize; }
    int GetTotalBatchNumber() const { return m_totalBatchNumber; }

private:
    int DumpProcessMemory(std::string idValue = "", bool bVerbose = false) const;
    int GetRecordSize(std::shared_ptr<arrow::Schema> schema, int defaultSize = 32) const;
    int GetStringRecordSize(std::shared_ptr<arrow::Schema> schema, int defaultSize = 32) const;
    arrow::Status ReadNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& table);
    arrow::Status Table2CSVBuffer(std::shared_ptr<arrow::Table>& table, int& buffSize, void* buf);
    int GetTotalRowNumber(std::string fileName) const;
    std::shared_ptr<arrow::ChunkedArray> ChunkArrayReformatBoolean(
            std::shared_ptr<arrow::ChunkedArray>& inputArray, bool bCheck = false);

    std::string m_path_to_file;
    std::string m_part_config_file;
    int m_maxMemory, m_recordSize, m_recordBufferSize;
    int m_vmRSS_init;
    int m_batchNumber, m_batchSize;
    int m_totalBatchNumber;
    int m_maxBufferSize;

    std::vector<std::string> m_parameterNames;
    std::unique_ptr<parquet::arrow::FileReader> m_arrow_reader_gbl;
    std::unique_ptr<::arrow::RecordBatchReader> m_rb_reader_gbl;
};
}  // namespace lsst::partition
