from sparta_pipeline import transformations
from sparta_pipeline import extract_files
import unittest
import pandas as pd
from pprint import pprint

pd.set_option("display.max_rows", None, "display.max_columns", None)


class TransformationTests(unittest.TestCase):

    def test_convert_si(self):
        test_dict = {'name': 'Hilary Willmore',
                     'date': '01/08/2019',
                     'tech_self_score': {'Python': 1, 'C#': 4, 'Java': 2, 'C++': 4},
                     'strengths': ['Patient', 'Curious', 'Problem Solving'],
                     'weaknesses': ['Overbearing', 'Chatty', 'Indifferent'],
                     'self_development': 'No',
                     'geo_flex': 'Yes',
                     'financial_support_self': 'Yes',
                     'result': 'Fail',
                     'course_interest': 'Data'}
        output_dict = {'name': 'Hilary Willmore',
                       'date': '01/08/2019',
                       'self_development': 0,
                       'geo_flex': 1,
                       'financial_support_self': 1,
                       'result': 0}
        pd.testing.assert_frame_equal(transformations.convert_si(test_dict), pd.DataFrame(output_dict, index=[0]))

    def test_convert_scores(self):
        test = ['Wednesday 18 September 2019',
                'London Academy',
                '',
                'PAULITA SIMMONDS -  Psychometrics: 53/100, Presentation: 14/32']
        output = pd.DataFrame([["PAULITA SIMMONDS", 53, 100, 14, 32]])
        pd.testing.assert_frame_equal(transformations.convert_scores(test), output)

