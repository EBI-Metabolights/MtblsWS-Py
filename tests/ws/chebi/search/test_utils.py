import unittest

from app.ws.chebi.search import utils


class UtilsTest(unittest.TestCase):

    def test_decode_compound_name_updated_1(self):
        input_data = "__a__b__"
        expected = "/a/b/"
        output = utils.decode_compound_name(input_data)
        self.assertEqual(expected, output)

    def test_decode_compound_name_updated_2(self):
        input_data = "_&_a_&_b_&_c_&_"
        expected = ".a.b.c."
        output = utils.decode_compound_name(input_data)
        self.assertEqual(expected, output)

    def test_decode_compound_name_updated_3(self):
        input_data = "__a__b_&_c___&_"
        expected = "/a/b.c/."
        output = utils.decode_compound_name(input_data)
        self.assertEqual(expected, output)

    def test_decode_compound_name_not_updated_1(self):
        input_data = "_a_b_&c_&"
        expected = input_data
        output = utils.decode_compound_name(input_data)
        self.assertEqual(expected, output)

    def test_decode_compound_name_not_updated_2(self):
        input_data = None
        expected = None
        output = utils.decode_compound_name(input_data)
        self.assertIsNone(output)

    def test_decode_compound_name_not_updated_3(self):
        input_data = "gibberellina98 gibberellina98"
        expected = input_data
        output = utils.decode_compound_name(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_whitespace_01(self):
        input_data = "gibberellina98 gibberellina98"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_whitespace_02(self):
        input_data = "gibberellina98    gibberellina98"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_whitespace_03(self):
        input_data = "gibberellina98\ngibberellina98"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_whitespace_04(self):
        input_data = "gibberellina98    \n  gibberellina98  \n  "
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_hyphen_01(self):
        input_data = "-- -gibberellina98-gibberellina98  \n  - --"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_underscode_01(self):
        input_data = "__ gibberellina98_gibberellina98  \n  __"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_comma_01(self):
        input_data = ", gibberellina98,gibberellina98  ,"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_parenthesis_01(self):
        input_data = "(gibberellina98),(gibberellina98)"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_square_brackets_01(self):
        input_data = "[gibberellina98],[(gibberellina98)]"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_curly_brackets_01(self):
        input_data = "{[gibberellina98]},{[(gibberellina98)]}"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_special_chars_01(self):
        input_data = "{[gibberellina98\u2013]},{[(gibberellina98\u2014)]}"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_quotation_01(self):
        input_data = "'{[gibberellina98]}','{[(gibberellina98)]}'"
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_remove_few_characters_for_consistency_quotation_02(self):
        input_data = '"{[gibberellina98]}","{[(gibberellina98)]}"'
        expected = "gibberellina98gibberellina98"
        output = utils.remove_few_characters_for_consistency(input_data)
        self.assertEqual(expected, output)

    def test_safe_split_string_regular(self):
        input_data = '"{[gibberellina98]}"|"{[(gibberellina98)]}"|test'
        expected = 3
        output = utils.safe_split_string(input_data)
        self.assertEqual(3, len(output))

    def test_safe_split_string_none(self):
        input_data = None
        expected = None
        output = utils.safe_split_string(input_data)
        self.assertIsNone(output)

    def test_safe_split_string_without_split_char(self):
        input_data = '"gibberellina98'
        expected = input_data
        expected_size = 1
        output = utils.safe_split_string(input_data)
        self.assertEqual(expected_size, len(output))
        self.assertEqual(expected, output[0])

    def test_safe_split_string_with_split_char_at_end(self):
        input_data = 'gibberellina98|'
        expected = "gibberellina98"
        expected_size = 1
        output = utils.safe_split_string(input_data)
        self.assertEqual(expected_size, len(output))
        self.assertEqual(expected, output[0])

    def test_safe_split_string_with_split_char_at_start_and_end_1(self):
        input_data = '|gibberellina98|'
        expected = "gibberellina98"
        expected_size = 1
        output = utils.safe_split_string(input_data)
        self.assertEqual(expected_size, len(output))
        self.assertEqual(expected, output[0])

    def test_safe_split_string_with_split_char_at_start_and_end_2(self):
        input_data = '|gibberellina98|test|'
        expected = "gibberellina98"
        expected_size = 2
        output = utils.safe_split_string(input_data)
        self.assertEqual(expected_size, len(output))
        self.assertEqual(expected, output[0])

    def test_find_term_index_in_source_01(self):
        input_data = '|gibberellina98|test|'
        input_match = "test"
        expected = 1
        output = utils.find_term_index_in_source(input_data, input_match)
        self.assertEqual(expected, output)

    def test_find_term_index_in_source_02(self):
        input_data = 'gibberellina98'
        input_match = "gibberellina98"
        expected = 0
        output = utils.find_term_index_in_source(input_data, input_match)
        self.assertEqual(expected, output)

    def test_find_term_index_in_source_03(self):
        input_data = 'gibberellina98|etst|test2'
        input_match = "test2"
        expected = 2
        output = utils.find_term_index_in_source(input_data, input_match)
        self.assertEqual(expected, output)

    def test_find_term_index_in_source_none(self):
        input_data = ""
        input_match = "gibberellina98x"
        expected = -1
        output = utils.find_term_index_in_source(input_data, input_match)
        self.assertEqual(expected, output)

    def test_find_term_index_in_source_not_found_01(self):
        input_data = 'gibberellina98'
        input_match = "gibberellina98x"
        expected = -1
        output = utils.find_term_index_in_source(input_data, input_match)
        self.assertEqual(expected, output)

    def test_get_term_in_source_1(self):
        input_data = 'gibberellina98|etst|test2'
        input_index = 2
        expected = "test2"
        output = utils.get_term_in_source(input_data, input_index)
        self.assertEqual(expected, output)

    def test_get_term_in_source_2(self):
        input_data = '|gibberellina98|etst|test2|'
        input_index = 2
        expected = "test2"
        output = utils.get_term_in_source(input_data, input_index)
        self.assertEqual(expected, output)

    def test_get_term_in_source_3(self):
        input_data = '|gibberellina98|etst|test2|'
        input_index = 0
        expected = "gibberellina98"
        output = utils.get_term_in_source(input_data, input_index)
        self.assertEqual(expected, output)

    def test_get_term_in_source_4(self):
        input_data = '|gibberellina98|'
        input_index = 0
        expected = "gibberellina98"
        output = utils.get_term_in_source(input_data, input_index)
        self.assertEqual(expected, output)

    def test_get_term_in_source_5(self):
        input_data = 'gibberellina98'
        input_index = 0
        expected = "gibberellina98"
        output = utils.get_term_in_source(input_data, input_index)
        self.assertEqual(expected, output)

    def test_get_term_in_source_6(self):
        input_data = 'gibberellina98'
        input_index = 1
        output = utils.get_term_in_source(input_data, input_index)
        self.assertIsNone(output)
