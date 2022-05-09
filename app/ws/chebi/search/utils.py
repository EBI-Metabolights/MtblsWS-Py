import re

dash_characters = "\u002D|\u058A|\u05BE|\u1400|\u1806|\u2010|\u2011|\u2012|\u2013|\u2014|\u2015|" \
                  "\u2053|\u207B|\u208B|\u2212|\u2E1A|\u2E3A|\u2E3B|\u2E40|\u301C|\u3030|\u30A0|" \
                  "\uFE31|\uFE32|\uFE58|\uFE63|\uFF0D"
special_chars = r"\s+|\-|_|,|\'|\[|\]|(|)|{|}|\""
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
    modified = re.sub(search_pattern, '', term)
    return modified


def find_term_index_in_source(source: str, match_term: str):
    if not source or not match_term:
        return -1
    ref_data = match_term.lower().strip()
    if "|" in source:
        search_terms = safe_split_string(source)
        for term in search_terms:
            if term.lower().strip() == ref_data:
                return search_terms.index(term)
    else:
        if source.lower().strip() == ref_data:
            return 0
    return -1


def safe_split_string(source, split_char: str = "|"):
    if not source:
        return source
    search_terms = source.split(split_char)
    search_terms = [x for x in search_terms if x]
    return search_terms


def get_term_in_source(source: str, index: int):
    if index < 0:
        return None
    if "|" in source:
        search_terms = safe_split_string(source)
        if len(search_terms) >= index:
            return search_terms[index]
        else:
            return None
    else:
        if index == 0:
            return source
        else:
            return None
