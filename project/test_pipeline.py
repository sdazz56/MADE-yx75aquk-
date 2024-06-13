import os
import unittest
import pandas as pd
import sqlite3
from unittest.mock import patch
from project.pipeline import download_file, save_to_csv, save_to_sqlite


class TestPipeline(unittest.TestCase):
    def setUp(self):
        # Define mock data
        self.mock_df = pd.DataFrame({
            "column1": [1, 2, 3],
            "column2": [4, 5, 6]
        })

        # Define file paths 
        self.output_dir = "output"
        self.csv_path = os.path.join(self.output_dir, "test_data.csv")
        self.db_path = os.path.join(self.output_dir, "test_data.db")

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    @patch("pipeline.pipeline.requests.get")
    def test_download_file(self, mock_get):
        # Mock the response from requests.get
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b"mock content"

        # Test downloading a file
        download_file("http://example.com/mockfile", self.csv_path)
        self.assertTrue(os.path.exists(self.csv_path))

    def test_save_to_csv(self):
        # Test saving DataFrame to CSV
        save_to_csv(self.mock_df, self.csv_path)
        self.assertTrue(os.path.exists(self.csv_path))

        # Read the saved CSV and compare with the original DataFrame
        df_read = pd.read_csv(self.csv_path)
        pd.testing.assert_frame_equal(df_read, self.mock_df)

    def test_save_to_sqlite(self):
        # Test saving DataFrame to SQLite
        save_to_sqlite(self.mock_df, self.db_path, "test_table")
        self.assertTrue(os.path.exists(self.db_path))

        # Read the saved data from SQLite and compare with the original DataFrame
        conn = sqlite3.connect(self.db_path)
        df_read = pd.read_sql("SELECT * FROM test_table", conn)
        conn.close()
        pd.testing.assert_frame_equal(df_read, self.mock_df)

    def tearDown(self):
        # Remove test files after each test
        if os.path.exists(self.csv_path):
            os.remove(self.csv_path)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)


if __name__ == "__main__":
    unittest.main()
