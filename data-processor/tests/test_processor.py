# Test cases for processor
import unittest
import pandas as pd
import tempfile
import os
from src.processor import DataProcessor
from src.utils import analyze_csv, find_merge_keys


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.main_data = pd.DataFrame({
            'i': [1, 2, 3],
            'j': [4, 5, 6],
            'k': [7, 8, 9],
            'value': [10, 20, 30]
        })
        self.aux_data = pd.DataFrame({
            'country_code': [1, 2, 3],
            'country_name': ['A', 'B', 'C']
        })
        self.main_file = os.path.join(
            self.temp_dir, 'BACI_HS02_Y2020_V202401b.csv')
        self.aux_file = os.path.join(self.temp_dir, 'country_codes.csv')
        self.main_data.to_csv(self.main_file, index=False)
        self.aux_data.to_csv(self.aux_file, index=False)

        self.output_file = os.path.join(self.temp_dir, 'output.csv')
        self.processor = DataProcessor(self.temp_dir, self.output_file)

    def test_analyze_files(self):
        self.processor.analyze_files()
        self.assertEqual(len(self.processor.main_files), 1)
        self.assertEqual(len(self.processor.auxiliary_files), 1)
        self.assertIn(self.aux_file, self.processor.merge_info)

    def test_process_data(self):
        self.processor.process_data()
        self.assertTrue(os.path.exists(self.output_file))
        result = pd.read_csv(self.output_file)
        self.assertGreater(len(result), 0)
        self.assertIn('country_name', result.columns)

    def test_analyze_csv(self):
        result = analyze_csv(self.main_file)
        self.assertIn('columns', result)
        self.assertIn('dtypes', result)
        self.assertIn('sample', result)

    def test_find_merge_keys(self):
        main_structure = analyze_csv(self.main_file)
        aux_structure = analyze_csv(self.aux_file)
        common_cols, potential_renames = find_merge_keys(
            main_structure, aux_structure)
        self.assertEqual(len(potential_renames), 1)
        self.assertEqual(potential_renames[0], ('i', 'country_code'))

    def tearDown(self):
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)


if __name__ == '__main__':
    unittest.main()
