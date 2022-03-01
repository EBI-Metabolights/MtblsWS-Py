from flask_restful import reqparse


class RequestParsers:


    @staticmethod
    def username_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('username', help='The username of the user we\'re retrieving information of.')
        return parser

    @staticmethod
    def study_files_tree_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('type', help='Which type of visualisation - tree or list')
        
    @staticmethod
    def europepmc_report_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('google_drive', help='Save the report to google drive instead of the vm?')

    @staticmethod
    def study_type_report_parser():
        parser = reqparse.RequestParser()
        parser.add_argument('studytype', help='The type of assay of the study IE NMR')
        return parser