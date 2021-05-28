from flask_restful import reqparse


class RequestParsers:


    @staticmethod
    def username_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('username', help='The username of the user we\'re retrieving information of.')
        return parser