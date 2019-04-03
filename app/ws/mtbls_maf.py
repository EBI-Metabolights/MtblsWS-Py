import logging, json, pandas as pd
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import create_maf, read_tsv

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


# Convert panda DataFrame to json tuples object
def totuples(df, text):
    d = [
        dict([
            (colname, row[i])
            for i, colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
    return {text: d}


def get_table_header(table_df):
    # Get an indexed header row
    df_header = pd.DataFrame(list(table_df))  # Get the header row only
    df_header = df_header.reset_index().to_dict(orient='list')
    mapping = {}
    print(df_header)
    for i in range(0, len(df_header['index'])):
        mapping[df_header[0][i]] = df_header['index'][i]
    return mapping


def insert_row(idx, df, df_insert):
    return df.iloc[:idx, ].append(df_insert, ignore_index=True).append(df.iloc[idx:, ]).reset_index(drop=True)


class MtblsMAFSearch(Resource):
    """Get MAF from studies (assays)"""
    @swagger.operation(
        summary="Search for metabolite onto_information to use in the Metabolite Annotation file",
        nickname="MAF search",
        notes="Get a given MAF associated with assay {assay_id} for a MTBLS Study with {study_id} in JSON format",
        parameters=[
            {
                "name": "query_type",
                "description": "The type of data to search for",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
                "enum": ["name", "databaseid", "inchi", "smiles"]
            },
            {
                "name": "search_value",
                "description": "The search string",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The metabolite search result is returned"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, query_type):
        # param validation
        if query_type is None:
            abort(404)

        parser = reqparse.RequestParser()
        parser.add_argument('search_value', help="The search string")
        search_value = None
        if request.args:
            args = parser.parse_args(req=request)
            search_value = args['search_value']

        if search_value is None:
            abort(404)

        search_res = wsc.get_maf_search(query_type, search_value)
        return search_res


class MetaboliteAnnotationFile(Resource):
    """Get MAF from filesystem"""
    @swagger.operation(
        summary="Read, and add missing samples for a MAF",
        nickname="Get MAF for a given MTBLS Assay",
        notes='''Get a given Metabolite Annotation File for a MTBLS Study with in JSON format. For assay_file_tech use 
<pre><code> 
{  
  "data": [ 
    {
      "assay_file_name": "a_some_assay_file.txt", 
      "technology": "MS"
    } 
  ]
}
</code></pre>''',
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "assay_file_tech",
                "description": "Assay File name and technology type (MS/NMR)",
                "required": True,
                "allowMultiple": False,
                "paramType": "body",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            },
            {
                "code": 417,
                "message": "Incorrect parameters provided"
            }
        ]
    )
    def post(self, study_id, annotation_file_name):
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            assay_file_tech = data_dict['data']
        except KeyError:
            assay_file_tech = None

        if assay_file_tech is None:
            abort(417, "Please provide valid data for updated new row(s). "
                       "The JSON string has to have a surrounding 'data' element")

        # param validation
        if study_id is None or annotation_file_name is None:
            abort(417)

        assay_file_name = assay_file_tech[0]['assay_file_name']
        technology = assay_file_tech[0]['assay_file_name']

        # param validation
        if assay_file_name is None or technology is None:
            abort(417, 'Please ensure the JSON has "assay_file_name" and "technology" elements')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('MAF: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        maf_df = create_maf(technology, study_location, assay_file_name, annotation_file_name)

        # Get an indexed header row
        df_header = get_table_header(maf_df)

        # Get the rows from the maf
        df_data_dict = totuples(maf_df.reset_index(), 'rows')
        return {'header': df_header, 'data': df_data_dict}

