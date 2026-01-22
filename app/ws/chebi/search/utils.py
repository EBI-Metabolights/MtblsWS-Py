import re

dash_characters = (
    "\u002d|\u058a|\u05be|\u1400|\u1806|\u2010|\u2011|\u2012|\u2013|\u2014|\u2015|"
    "\u2053|\u207b|\u208b|\u2212|\u2e1a|\u2e3a|\u2e3b|\u2e40|\u301c|\u3030|\u30a0|"
    "\ufe31|\ufe32|\ufe58|\ufe63|\uff0d"
)
special_chars = r"\s|\-|_|,|\'|\[|\]|(|)|{|}|\""
search_regex = f"[{dash_characters}|{special_chars}]"
search_pattern = re.compile(search_regex)


def decode_compound_name(name: str):
    if not name:
        return name
    decoded_name = name
    decoded_name = decoded_name.replace("__", "/")
    decoded_name = decoded_name.replace("_&_", ".")
    return decoded_name


def remove_few_characters_for_consistency(term):
    modified = re.sub(search_pattern, "", term)
    return modified


def find_term_index_in_source(source: str, match_term: str):
    if not source or not match_term:
        return -1
    match_term_filtered = remove_few_characters_for_consistency(
        match_term.lower().strip()
    )
    if "|" in source:
        search_terms = safe_split_string(source)
        for term in search_terms:
            term_filtered = remove_few_characters_for_consistency(term.lower().strip())
            if term_filtered == match_term_filtered:
                return search_terms.index(term)
    else:
        source_filtered = remove_few_characters_for_consistency(source.lower().strip())
        if source_filtered == match_term_filtered:
            return 0
    return -1


def safe_split_string(source, split_char: str = "|"):
    if not source:
        return source
    search_terms = source.split(split_char)
    search_terms = [x for x in search_terms if x]
    return search_terms


def get_term_in_source(source: str, index: int):
    if index < 0 or not isinstance(source, str) or not source:
        return None
    if "|" in source:
        search_terms = safe_split_string(source)
        if search_terms and len(search_terms) >= index:
            return search_terms[index]
        else:
            return None
    else:
        if index == 0:
            return source
        else:
            return None
