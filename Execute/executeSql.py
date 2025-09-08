from route import psycopg2
from Execute import responses,middleware
from psycopg2.extensions import connection as _connection
# from route import get_db_connection
from db import get_db_connection
import psycopg2.extras
from Execute import responses, middleware
from psycopg2.extras import RealDictCursor
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

# SELECT / DELETE / INSERT MANY
def ExecuteAll(query, data=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data or ())
        results = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        print("Error in ExecuteAll:", e)
        return None





# Execute a SELECT query that returns a single row
def ExecuteReturn(query, data=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data or ())
        result = cur.fetchone()  # single row or None
        conn.commit()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print("Error in ExecuteReturn:", e)
        return None



# INSERT/UPDATE/DELETE one record (no return)
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

# def ExecuteOne(query, data=None):
#     try:
#         conn = get_db_connection()
#         cur = conn.cursor()
#         cur.execute(query, data or ())
#         result = cur.fetchone()
#         conn.commit()
#         cur.close()
#         conn.close()
#         return result  # tuple or None
#     except Exception as e:
#         print("Error in ExecuteOne:", e)
#         return None

# Execute an INSERT, UPDATE, DELETE or any query

def ExecuteOne(sql, params=None, return_rowcount=False):
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, params or ())

        if return_rowcount:
            affected = cur.rowcount
            conn.commit()
            return affected

        # for SELECT queries
        row = cur.fetchone()
        conn.commit()
        return row

    except Exception as e:
        if conn:
            conn.rollback()
        raise e

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
def FetchOne(query, data=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, data)
        row = cur.fetchone()
        cur.close()
        conn.close()

        return {
            "code": "1024400",
            "successmsgs": "Fetched Successfully",
            "data": row if row else {}
        }

    except Exception as e:
        print("Error in FetchOne ==============================", e)
        return {
            "code": "1024500",
            "errmsgs": "Fetching Failed",
            "error": str(e)
        }

    

def FetchAll(query, params=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(query, params or ())
        rows = cur.fetchall()

        cur.close()
        conn.close()

        # convert to list of dicts (JSON serializable)
        return [dict(row) for row in rows]

    except Exception as e:
        print("Error in FetchAll==============================", e)
        return []


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
def ExecuteReturn(query, data=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, data or ())
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return result  # tuple or None
    except Exception as e:
        print("Error in ExecuteReturn:", e)
        return None
    
    
def ExecuteRowCount(query, params=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, params or ())
        rowcount = cur.rowcount  # <-- number of rows affected
        conn.commit()
        cur.close()
        conn.close()
        return rowcount
    except Exception as e:
        print("Error in ExecuteRowCount:", e)
        return 0




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
    


    # SELECT ALL with headers (safe version for IRR queries)
    #  """
    # Executes a SQL query and returns all rows as a list of dicts.
    # Always returns a list (empty if no rows) to avoid errors.
    # """



def ExecuteAllWithHeaders(query, params=None):
    """
    Executes a query and returns a list of dicts with column headers.
    Handles TEXT/NUMERIC columns reliably.
    Never returns a Response object, always a list of dicts.
    """
    conn = None
    cur = None
    try:
        conn = get_db_connection()  # Your existing DB connection function
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params or ())
        rows = cur.fetchall()

        if not rows:
            return []

        result = []
        for row in rows:
            clean_row = {}
            for k, v in row.items():
                if v is None:
                    clean_row[k] = None
                elif isinstance(v, (int, float)):
                    clean_row[k] = v
                else:
                    # Try auto-convert numeric strings to float
                    try:
                        clean_row[k] = float(v)
                    except (ValueError, TypeError):
                        clean_row[k] = v
            result.append(clean_row)

        return result

    except Exception as e:
        print("Error in ExecuteAllWithHeaders =============================", e)
        return []

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()




            