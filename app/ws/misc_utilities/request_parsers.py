from flask_restful import reqparse


class RequestParsers:


    @staticmethod
    def username_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('username', help='The username of the user we\'re retrieving information of.')
        return parser

    @staticmethod
    def europepmc_report_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('google_drive', help='Save the report to google drive instead of the vm?')
        return parser