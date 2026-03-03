from app.ws.db_connection import create_empty_study, get_connection


def execute_query_with_parameter(query, parameters):
    with get_connection() as (conn, cursor):
        try:
            cursor.execute(query, parameters)
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            return None


class UserTestData:
    def __init__(self, user_token, email, userid, role, status, studies=None):
        self.user_token = user_token
        self.email = email
        self.userid = userid
        if not studies:
            studies = []
        self.studies = studies
        self.role = role
        self.status = status


def create_user_in_db(user):
    insert_user_sql = """
        INSERT INTO users (id, apitoken, email, password, role, status, username)
        VALUES (%(id)s, %(apitoken)s, %(email)s, %(password)s, %(role)s, %(status)s, %(username)s);"""
    content1 = {
        "id": user.userid,
        "apitoken": user.user_token,
        "email": user.email,
        "password": "",
        "status": str(user.status),
        "role": user.role,
        "username": user.email,
    }
    execute_query_with_parameter(insert_user_sql, content1)


def create_test_study_in_db(user, study_id):
    create_empty_study(user.user_token, study_id)


def delete_test_user_from_db(user):
    sql = "delete from study_user where userid = %(userid)s;"
    params = {"userid": user.userid}
    execute_query_with_parameter(sql, params)

    sql = "delete from users where email = %(email)s;"
    params = {"email": user.email}
    execute_query_with_parameter(sql, params)


def delete_test_study_from_db(study_id):
    sql = "delete from study_user where studyid in (select id from studies where acc = %(acc)s);"
    params = {"acc": study_id}
    execute_query_with_parameter(sql, params)

    sql = "delete from studies where acc = %(acc)s;"
    params = {"acc": study_id}
    execute_query_with_parameter(sql, params)
