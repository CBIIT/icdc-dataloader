import unittest
from s3_loader import *
from os.path import isfile, join


class TestFileLoader(unittest.TestCase):

    def setUp(self):
        self.file_location = "./data/FileLoaderTestData"
        self.isb = "yizhen-file-loader"
        self.isf = "input"
        self.osb = "yizhen-file-loader"
        self.osf = "output"
        self.i = "bolt://localhost:7687"
        self.u = "neo4j"
        self.s1 = "./test/data/icdc-model.yml"
        self.s2 = "./test/data/icdc-model-props.yml"
        self.md5 = "65536"
        self.f = "0"
        self.python = "./env/bin/python3"
        self.manual = "false"
        self.p = os.environ['NEO_PASSWORD']

    def test_check_manifest(self):
        self.assertTrue(check_manifest(join(self.file_location, "correct_input.txt"), self.file_location))
        self.assertFalse(check_manifest(join(self.file_location, "input_with_file_no_exist.txt"), self.file_location))
        self.assertFalse(check_manifest(join(self.file_location, "input_with_null_fileName.txt"), self.file_location))
        self.assertFalse(check_manifest(join(self.file_location, "input_with_null_case_id.txt"), self.file_location))

    def test_check_file_exist(self):
        self.assertTrue(check_file_exist(self.file_location))
        self.assertFalse(check_file_exist("./data/FileLoaderTestData_Wrong"))

    def test_file_loader(self):
        cmd1 = " cd .. ;"
        cmd2 = self.python + " ./s3_loader.py" + " -t " + join("./test/data/FileLoaderTestData","correct_input.txt") + " -d " +"./test/data/FileLoaderTestData" + " -isb " + self.isb + " -isf " + self.isf + " -osb " + self.osb + " -osf " + self.osf + " -i " + self.i + " -u " + self.u + " -s " + self.s1 + " -s " + self.s2 + " -md5 " + self.md5 + " -f " + self.f + " -python " + self.python + " -manual " + self.manual+ " -p " + self.p
        print("Command Line {}".format(cmd2))
        cmd = cmd1 + cmd2
        process_status = subprocess.run(cmd, shell=True)
        self.assertTrue(process_status.returncode == 0)


if __name__ == '__main__':
    unittest.main()
