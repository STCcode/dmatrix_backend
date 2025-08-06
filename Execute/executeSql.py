from route import psycopg2
from Execute import responses,middleware
from psycopg2.extensions import connection as _connection
# from route import get_db_connection
from db import get_db_connection
from Execute import responses, middleware
#for Selecting All record

# def ExecuteAll(query, data):
#     try:
#         conn = get_db_connection()
#         cur = conn.cursor()
#         cur.execute(query, data)
#         result = cur.fetchall()
#         conn.commit()
#         cur.close()
#         conn.close()
#         return result
#     except Exception as e:
#         print("Error in ExecuteAll=============================", e)
#         return middleware.exe_msgs(responses.execution_501, str(e.args), '1023300')
# # def ExecuteAll(query,data):
# #     try:
# #         cur = psycopg2.connection.cursor()
# #         cur.execute(query,data)
# #         result = cur.fetchall()
# #         psycopg2.connection.commit()
# #         cur.close()
# #         return result
# #     except Exception as e:
# #                 print("Error in ExecuteAll=============================",e)
# #                 return middleware.exe_msgs(responses.execution_501,str(e.args),'1023300')

# #for selecting one record
# def ExecuteReturn(query,data):
#     try:
#         cur = psycopg2.connection.cursor()
#         cur.execute(query,data)
#         result = cur.fetchone()
#         psycopg2.connection.commit()
#         cur.close()
#         return result
#     except Exception as e:
#                 print("Error in ExecuteReturn============================",e)
#                 return middleware.exe_msgs(responses.execution_501,str(e.args),'1022300')


# #for executing one record with no return data
# # def ExecuteOne(query,data):
# #     try:
# #         cur = psycopg2.connection.cursor()
# #         cur.execute(query,data)
# #         result=responses.execution_200
# #         psycopg2.connection.commit()
# #         cur.close()
# #         return result
# #     except Exception as e:
# #                 print("Error in ExecuteOne==============================",e)
# #                 return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')
    
# def ExecuteOne(query, data):
#     try:
#         conn = get_db_connection()
#         cur = conn.cursor()
#         cur.execute(query, data)
#         conn.commit()
#         cur.close()
#         conn.close()
#         return responses.execution_200
#     except Exception as e:
#         print("Error in ExecuteOne==============================", e)
#         return middleware.exe_msgs(responses.execution_501, str(e.args), '1020300')

# #for executing many record with no return data
# # def ExecuteMany(query,data):
# #     try:
# #         cur = psycopg2.connection.cursor()
# #         cur.executemany(query,data)
# #         result=responses.execution_200
# #         psycopg2.connection.commit()
# #         cur.close()
# #         return result
# #     except Exception as e:
# #                 print("Error in ExecuteMany==============================",e)
# #                 return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')

# #for returning Inserted Id
# # def ExecuteReturnId(query,data):
# #     try:
# #         cur = psycopg2.connection.cursor()
# #         cur.execute(query,data)
# #         id=psycopg2.connection.insert_id()
# #         result = id
# #         psycopg2.connection.commit()
# #         cur.close()
# #         return result
# #     except Exception as e:
# #                 print("Error in ExecuteReturnId=============================",e)
# #                 return middleware.exe_msgs(responses.execution_501,str(e.args),'1022300')



# # def ExecuteAllNew(query,data):
# #     try:
# #         cur = psycopg2.connection.cursor()
# #         cur.execute(query,data)
# #         results = cur.fetchall()
# #         if len(results) == 0:
# #             #return middleware.exe_msgs(responses.getAll_200,"No Record Found",'1023300')
# #             payload = []
# #             return payload
# #         row_headers=[x[0] for x in cur.description] #this will extract row headers
# #         psycopg2.connection.commit()
# #         cur.close()
# #         payload = []
# #         for result in results:
# #             payload.append(dict(zip(row_headers,result)))
# #         return payload
# #     except Exception as e:
# #         print("Error in ExecuteAll=============================",e)
# #         return middleware.exe_msgs(responses.execution_501,str(e.args),'1023300')

# SELECT ALL
def ExecuteAll(query, data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data)
        result = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print("Error in ExecuteAll=============================", e)
        return middleware.exe_msgs(responses.execution_501, str(e.args), '1023300')


# SELECT ONE
def ExecuteReturn(query, data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data)
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print("Error in ExecuteReturn============================", e)
        return middleware.exe_msgs(responses.execution_501, str(e.args), '1022300')


# INSERT/UPDATE/DELETE one record (no return)
def ExecuteOne(query, data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()
        cur.close()
        conn.close()
        return responses.execution_200
    except Exception as e:
        print("Error in ExecuteOne==============================", e)
        return middleware.exe_msgs(responses.execution_501, str(e.args), '1020300')


# EXECUTE MANY
def ExecuteMany(query, data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.executemany(query, data)
        conn.commit()
        cur.close()
        conn.close()
        return responses.execution_200
    except Exception as e:
        print("Error in ExecuteMany==============================", e)
        return middleware.exe_msgs(responses.execution_501, str(e.args), '1020300')


# RETURN INSERTED ID
def ExecuteReturnId(query, data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data)
        inserted_id = cur.fetchone()[0]  # works with RETURNING id
        conn.commit()
        cur.close()
        conn.close()
        return inserted_id
    except Exception as e:
        print("Error in ExecuteReturnId=============================", e)
        return middleware.exe_msgs(responses.execution_501, str(e.args), '1022300')


# SELECT ALL with headers
def ExecuteAllNew(query, data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data)
        results = cur.fetchall()
        if len(results) == 0:
            return []
        row_headers = [x[0] for x in cur.description]
        conn.commit()
        cur.close()
        conn.close()
        payload = [dict(zip(row_headers, row)) for row in results]
        return payload
    except Exception as e:
        print("Error in ExecuteAllNew=============================", e)
        return middleware.exe_msgs(responses.execution_501, str(e.args), '1023300')
