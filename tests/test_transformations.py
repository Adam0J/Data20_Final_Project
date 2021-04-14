from sparta_pipeline import transformations
import unittest
import pandas as pd
from pprint import pprint

pd.set_option("display.max_rows", None, "display.max_columns", None)


class TransformationTests(unittest.TestCase):

    def test_convert_si(self):
        pass

    def test_convert_scores(self):
        pass

    def test_convert_pi(self):
        pass

    def test_convert_weeks(self):
        test_dict = {"name": "Pyotr De Zuani", "trainer": "Trixie Orange",
                     "Analytic_W1": 1, "Independent_W1": 3, "Determined_W1": 4, "Professional_W1": 2, "Studious_W1": 2,
                     "Imaginative_W1": 2}
        test_pd = pd.DataFrame(test_dict, index=[0])
        result_dict = {"week_id": [1, 1, 1, 1, 1, 1], "behaviour_id": [1, 2, 3, 4, 5, 6], "score": [1, 3, 4, 2, 2, 2]}
        result_pd = pd.DataFrame(result_dict)
        self.assertEqual(transformations.convert_weeks(test_pd), result_pd)


    def test_convert_courses(self):
        pass
