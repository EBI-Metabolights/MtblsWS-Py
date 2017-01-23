import os
import json
from jsonschema import RefResolver, Draft4Validator

"""
METASPACE MaoriValidator

Validator using the JSON schema for METASPACE Maori

author: jrmacias
date: 20170123
"""


class MaoriValidator:

    def validate_json(self, json_file, schema_file):
        assert os.path.exists(schema_file), "Did not find JSON schema file: %s" % schema_file
        assert os.path.exists(json_file), "Did not find JSON file: %s" % json_file

        schema = json.load(open(os.path.join(schema_file)))
        resolver = RefResolver(schema_file, schema)
        validator = Draft4Validator(schema, resolver=resolver)
        try:
            validator.validate(json.load(open(json_file)), schema)
        except Exception as e:
            print("Oops! Got an invalid json file:", json_file)
            return
        else:
            print("Fine! Got a valid json file:", json_file)
        return


# schema_path = os.path.normpath(os.path.join("../", "model/maori_schema.json"))
# mVal = MaoriValidator()
# mVal.validate_json(os.path.normpath(os.path.join("../", "test_data/20161024_maori_sample.json")), schema_path)
# mVal.validate_json(os.path.normpath(os.path.join("../", "test_data/passing_sample_file.json")), schema_path)
# mVal.validate_json(os.path.normpath(os.path.join("../", "test_data/failing_sample_file.json")), schema_path)
