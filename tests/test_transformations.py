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
                'PAULITA SIMMONDS -  Psychometrics: 53/100, Presentation: 14/32', ]
        output = pd.DataFrame([[53, 100, 14, 32]])
        pd.testing.assert_frame_equal(transformations.convert_scores(test), output)

    def test_convert_weeks(self):
        test_dict = {"name": "Pyotr De Zuani", "trainer": "Trixie Orange",
                     "Analytic_W1": 1, "Independent_W1": 3, "Determined_W1": 4, "Professional_W1": 2, "Studious_W1": 2,
                     "Imaginative_W1": 2}
        test_pd = pd.DataFrame(test_dict, index=[0])
        result_dict = {"name": "Pyotr De Zuani", "trainer": "Trixie Orange",
                       "behaviours": ["Analytic", "Independent", "Determined", "Professional", "Studious",
                                      "Imaginative"],
                       "score": [1, 3, 4, 2, 2, 2],
                       "week_id": [1, 1, 1, 1, 1, 1]}
        result_pd = pd.DataFrame(result_dict)
        pd.testing.assert_frame_equal(transformations.convert_weeks(test_pd), result_pd)

    def test_convert_courses(self):
        test_dict = {"name": "Frederico D'Ambrosio", "date": "21/08/2019",
                     "tech_self_score": {'Python': 4, 'C#': 4, 'Java': 2, 'C++': 3},
                     'strengths': ['Patient', 'Problem Solving'],
                     'weaknesses': ['Overbearing', 'Indifferent'], "self_development": "Yes",
                     "geo_flex": "Yes", "financial_support_self": "No", "result": "Pass", "course_interest": "Data"}
        output_dict = {"name": "Data"}
        pd.testing.assert_frame_equal(transformations.convert_courses(test_dict), pd.DataFrame(output_dict, index=[0]))
