
from lsst.qserv.admin import commons
from lsst.qserv.qservdataloader import QservDataLoader
from lsst.qserv.datareader import DataReader
import os
import unittest

class TestQservDataLoader(unittest.TestCase):

    def setUp(self):
        self.config = commons.read_user_config()
        self.logger = commons.init_default_logger(
            "TestQservDataLoader",
            log_path=self.config['qserv']['log_dir']
            )
        
   
    def test_alterTable(self):
        case_id_list = ["01","02","03"]

        for case_id in case_id_list:

            qserv_tests_dirname = os.path.join(self.config['qserv']['base_dir'],'qserv','tests',"case%s" % case_id)
            input_dirname = os.path.join(qserv_tests_dirname,'data')

            dataReader = DataReader(input_dirname, "case%s" % case_id)
            dataReader.readInputData()

            test_name = "TestQservDataLoader%s" % case_id
            out_dir = os.path.join(self.config['qserv']['tmp_dir'],test_name)
            qservDataLoader = QservDataLoader(
                self.config,
                dataReader.dataConfig,
                test_name,
                out_dir
                )
            qservDataLoader.connectAndInitDatabase()
            for table_name in dataReader.dataConfig['partitionned-tables']:
                (schema_filename, data_filename, zipped_data_filename) =  dataReader.getSchemaAndDataFilenames(table_name)
                qservDataLoader._sqlInterface['cmd'].executeFromFile(schema_filename)
                qservDataLoader.alterTable(table_name)


if __name__ == '__main__':
    unittest.main()
