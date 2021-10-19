from flask_restful import reqparse


class RequestParsers:
    """
    Collection of request parsers to prevent cluttering in code elsewhere.
    """


    @staticmethod
    def username_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('username', help='The username of the user we\'re retrieving information of.')
        return parser

    @staticmethod
    def study_validation_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('section', help="Validation section", location="args")
        parser.add_argument('level', help="Validation message levels", location="args")
        parser.add_argument('static_validation_file', help="Use pre-generated validations", location="args")
        return parser