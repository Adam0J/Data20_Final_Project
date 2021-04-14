from sparta_pipeline import extract_files
import unittest


class ExtractTests(unittest.TestCase):

    def test_csv(self):
        test = extract_files.extract_csv("test.json")
        self.assertEqual(test, "That is not a CSV file.")

    def test_json(self):
        test = extract_files.extract_json("test.csv")
        self.assertEqual(test, "That is not a JSON file.")

    def test_txt(self):
        test = extract_files.extract_txt("test.csv")
        self.assertEqual(test, "That is not a TEXT file.")
