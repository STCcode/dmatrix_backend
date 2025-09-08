from datetime import datetime
from Execute import executeSql,responses,middleware
import platform
from flask import jsonify, request
import numpy as np
import json  
#getting all user data
def getAllUserData():
     try:
          sql="SELECT * from tbl_user_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getAllUserData query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


#for login
# postgres query

def login_user(data):
    try:
        sql = "SELECT id, name, email FROM tbl_users WHERE email = %s AND password = md5(%s)"
        return executeSql.ExecuteAllNew(sql, data)
    except Exception as e:
        print("Error in login_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020320')

# postgres query
def save_user(data):
    try:
     #    sql = " INSERT INTO tbl_users (name, email, password, created_by, created_date, updated_date) VALUES (%s, %s, md5(%s), %s, %s, %s)"
        sql = " INSERT INTO tbl_users (name, email, password, created_date, updated_date) VALUES (%s, %s, md5(%s), %s, %s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')   
# Check Username

def checkusername(id):
     try:
          sql="SELECT * FROM tbl_user_master where s_login_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in check username ==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
#get all users

def getAlluser():
     try:
          sql="SELECT *,getrolename(s_role) as role_name FROM tbl_user_master"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
     
#getting area data by id
def getAllUserById(id):
     try:
          sql="select * from tbl_user_master where n_user_id=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in geeting area by id query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
     
#update user data
def updateuser(data):
     try:
          if len(data)==8:
               sql="UPDATE tbl_user_master SET s_login_id=%s,s_email=%s,s_password=%s,s_dep=%s,s_contact_no=%s,s_active=%s,s_role=%s WHERE n_user_id=%s"
          else:
               sql="UPDATE tbl_user_master SET s_login_id=%s,s_email=%s,s_dep=%s,s_contact_no=%s,s_active=%s,s_role=%s WHERE n_user_id=%s"

          msgs=executeSql.ExecuteOne(sql,data)
          return msgs
     except Exception as e:
          print("Error in update areaRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

#================================ User Master END===============================================

# def forgetpassword(id):
#      try:
#           sql="SELECT * FROM tbl_user where s_email=%s"
#           data={id}
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in geeting area by id query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')



# ==============================entity Table Start=====================================

# def get_next_entity_id():
#     try:
#         sql = "SELECT COALESCE(MAX(id), 0) + 1 FROM tbl_entity"
#         result = executeSql.ExecuteReturnId(sql)
#         return result[0][0]  # return next id number
#     except Exception as e:
#         print("Error in get_next_entity_id:", e)
#         return 1  # fallback to 1 if error


def entity_table(data):
    try:
        sql = "INSERT INTO tbl_entity (scripname, scripcode, entityID, benchmark, category,subcategory, nickname, created_at, isin) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')
    
    
def getAllentity():
     try:
          sql="SELECT * FROM tbl_entity"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
     


def getAllMutualFund():
     try:
          sql="SELECT * FROM tbl_entity WHERE category = 'Equity' AND subcategory = 'Mutual Fund';"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
     
def getMutualFundDataById(entity_id):
    try:
        sql = "SELECT * FROM tbl_entity  WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

def update_entity_table(data):
    try:
        sql = "UPDATE tbl_entity SET scripname = %s,scripcode = %s,benchmark = %s,category = %s,subcategory = %s,nickname = %s,isin =%s,updated_at = %s WHERE id = %s"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in update_entity_table query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020311')
    
def DeleteEntityByid(entity_id):
    try:
        sql = "DELETE FROM tbl_entity WHERE id = %s"
        data = (entity_id,) 
        deleted_count = executeSql.ExecuteReturnId(sql, data) 
        return deleted_count
    except Exception as e:
        print("Error in DeleteEntityByid query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')
  

def getCountOfAllEntity():
     try:
          sql=" SELECT MIN(subcategory) AS subcategory,MIN(category) AS category,COUNT(*) AS total FROM tbl_entity WHERE LOWER(subcategory) IN ('mutual fund', 'alternative investment funds', 'direct equity') AND LOWER(category) IN ('equity') GROUP BY LOWER(subcategory) ORDER BY subcategory;"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')




# def entity_table(data):
#     try:
#         sql = " INSERT INTO tbl_entity (scrip_name, scripcode, entity_Id, benchmark, category, sector, nickname, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
#         msg = executeSql.ExecuteReturnId(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in save_user query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')
    


# ==============================entity Table End=======================================



# ==================================== Action Table Start==================================
def action_table(data):
    try:
        sql = " INSERT INTO tbl_action_table (scrip_code, mode, order_type, scrip_name, isin, order_number, folio_number, nav, stt, unit, redeem_amount, purchase_amount, cgst, sgst, igst, ugst, stamp_duty, cess_value,net_amount,created_at,entityid,purchase_value,order_date,sett_no) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s,%s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')
    

def getAllAction():
     try:
          sql="SELECT * FROM tbl_action_table"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
     
def getActionByentId(entity_id):
    try:
        sql = "SELECT * FROM tbl_action_table WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')     


def getMfByentId():
     try:
          sql="SELECT e.*, a.* FROM tbl_entity e JOIN tbl_action_table a ON e.entityid = a.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund';"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')      



# ==================================== Action Table End====================================


# ==============================mcap Table Start=====================================

def mcap_table(data):
    try:
        sql = " INSERT INTO tbl_mcap (company_name, sector, symbol, series, isin_code, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

# ==============================mcap Table End=======================================


# ==============================Underlying Table Start=====================================

# def underlying_table(data):
#     try:
#         sql = " INSERT INTO tbl_Underlying (company_name, scripcode, weightage, sector, isin_code, created_at,entityid) VALUES (%s, %s, %s, %s, %s, %s,%s)"
#         msg = executeSql.ExecuteReturnId(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in save_user query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


def underlying_table(data):
    try:
        sql = "INSERT INTO tbl_Underlying (company_name, scripcode, weightage, sector, isin_code, created_at, entityid, tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in underlying_table query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

    
    
def getAllUnderlying():
     try:
          sql="SELECT * FROM tbl_underlying"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
     

# def getUnderlyingById(entity_id):
#      try:
#           sql="select * from tbl_underlying where entityid = '%s';"
#           data={entity_id,}
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in geeting area by id query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')     
    

def getUnderlyingById(entity_id):
    try:
        sql = "SELECT u.*, e.scripname FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')
    
def getUnderlyingByMf():
     try:
          sql="SELECT e.*, a.*, u.* FROM tbl_entity e JOIN tbl_action_table a ON e.entityid = a.entityid JOIN tbl_underlying u ON e.entityid = u.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund';"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')  

# def ClearUnderlyingdata(entity_id):
#     try:
#         result_summary = {}

#         # 1. Check if entityid exists in tbl_underlying
#         check_underlying_sql = "SELECT 1 FROM tbl_underlying WHERE entityid = %s"
#         underlying_exists = executeSql.ExecuteReturn(check_underlying_sql, (entity_id,))

#         if underlying_exists:
#             # Case A: entityid exists in tbl_underlying → delete it
#             delete_sql = "DELETE FROM tbl_underlying WHERE entityid = %s"
#             executeSql.ExecuteOne(delete_sql, (entity_id,))
#             result_summary["action"] = "deleted"
#             result_summary["rows_affected"] = 1

#         else:
#             # Case B: entityid not in tbl_underlying → check if it exists in tbl_entity
#             check_entity_sql = "SELECT 1 FROM tbl_entity WHERE entityid = %s"
#             entity_exists = executeSql.ExecuteReturn(check_entity_sql, (entity_id,))

#             if entity_exists:
#                 # Insert entityid into tbl_underlying
#                 insert_sql = "INSERT INTO tbl_underlying (entityid) VALUES (%s)"
#                 executeSql.ExecuteOne(insert_sql, (entity_id,))
#                 result_summary["action"] = "inserted"
#                 result_summary["rows_affected"] = 1
#             else:
#                 # Case C: entityid not in tbl_entity either → nothing to do
#                 result_summary["action"] = "not_found"
#                 result_summary["rows_affected"] = 0

#         return result_summary

#     except Exception as e:
#         print("Error in ClearUnderlyingdata query:", e)
#         return {
#             "action": "error",
#             "error": str(e),
#             "rows_affected": 0
#         }


# #####

# def ClearUnderlyingdata(entity_id):
#     try:
#         result_summary = {}

#         # 1. Check if entityid exists in tbl_underlying
#         check_underlying_sql = "SELECT 1 FROM tbl_underlying WHERE entityid = %s LIMIT 1"
#         underlying_exists = executeSql.ExecuteReturn(check_underlying_sql, (entity_id,))

#         if underlying_exists:
#             # Case A: delete all rows from tbl_underlying
#             delete_sql = "DELETE FROM tbl_underlying WHERE entityid = %s"
#             rows_deleted = executeSql.ExecuteRowCount(delete_sql, (entity_id,))
#             result_summary["action"] = "deleted"
#             result_summary["rows_affected"] = rows_deleted

#         else:
#             # Case B: check tbl_entity
#             check_entity_sql = "SELECT 1 FROM tbl_entity WHERE entityid = %s LIMIT 1"
#             entity_exists = executeSql.ExecuteReturn(check_entity_sql, (entity_id,))

#             if entity_exists:
#                 insert_sql = """
#                     INSERT INTO tbl_underlying 
#                         (company_name, scripcode, weightage, sector, isin_code, created_at, entityid)
#                     SELECT 
#                         scripname,       
#                         scripcode,       
#                         weightage,       
#                         sector,          
#                         isin,            
#                         NOW(),           
#                         entityid         
#                     FROM tbl_entity
#                     WHERE entityid = %s
#                 """
#                 rows_inserted = executeSql.ExecuteRowCount(insert_sql, (entity_id,))
#                 result_summary["action"] = "inserted"
#                 result_summary["rows_affected"] = rows_inserted
#             else:
#                 result_summary["action"] = "not_found"
#                 result_summary["rows_affected"] = 0

#         return result_summary

#     except Exception as e:
#         print("Error in ClearUnderlyingdata query:", e)
#         return {"action": "error", "error": str(e), "rows_affected": 0}


# queri.py

def ClearUnderlyingdata(entity_id):
    try:
        entity_id = entity_id.strip()

        # 1. Try delete first
        delete_sql = "DELETE FROM tbl_underlying WHERE TRIM(entityid) = TRIM(%s)"
        rows_deleted = executeSql.ExecuteOne(delete_sql, (entity_id,), return_rowcount=True)

        print(f"[DEBUG] Direct delete for {entity_id}, rows_deleted={rows_deleted}")

        if rows_deleted and rows_deleted > 0:
            return {
                "data": {
                    "message": f"Entity {entity_id} deleted from tbl_underlying",
                    "rows_affected": rows_deleted
                },
                "successmsgs": "Record(s) deleted successfully",
                "code": "1024200"
            }

        # 2. If no rows deleted, check if entity exists in tbl_entity
        check_sql = "SELECT 1 FROM tbl_entity WHERE TRIM(entityid) = TRIM(%s)"
        exists = executeSql.ExecuteReturn(check_sql, (entity_id,))

        if exists:
            insert_sql = "INSERT INTO tbl_underlying (entityid) VALUES (%s)"
            inserted_count = executeSql.ExecuteOne(insert_sql, (entity_id,), return_rowcount=True)

            return {
                "data": {
                    "message": f"Entity {entity_id} inserted into tbl_underlying",
                    "rows_affected": inserted_count
                },
                "successmsgs": "Inserted Successfully",
                "code": "1024201"
            }
        else:
            return {
                "data": {
                    "message": f"Entity {entity_id} not found in tbl_entity",
                    "rows_affected": 0
                },
                "successmsgs": "No matching entity found",
                "code": "1024204"
            }

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print("🔥 ClearUnderlyingdata failed:", error_details)

        return {
            "errmsgs": f"Query error: {str(e)}",
            "error": error_details,
            "code": "1024503"
        }

# ####

# ==============================Underlying Table End =======================================

     

# ==============================bigsheet Table Start =======================================   
    

def getCamByid(company_name=None):
    try:
          # sql = "SELECT name_of_company, isin_number FROM bigsheet_data WHERE name_of_company ILIKE %s"
          # sql = "SELECT issuer_name,name_of_company, isin,sector_name,tag FROM equity_bigsheet_data WHERE name_of_company ILIKE %s;"
          sql = "SELECT CASE WHEN issuer_name IS NOT NULL AND name_of_company IS NOT NULL AND normalize_company_name(issuer_name) <> normalize_company_name(name_of_company) THEN issuer_name || ' / ' || name_of_company ELSE COALESCE(issuer_name, name_of_company) END AS company_name,isin,sector_name,tag FROM equity_bigsheet_data WHERE normalize_company_name(COALESCE(name_of_company, issuer_name)) ILIKE normalize_company_name(%s);"
          data = (f"%{company_name}%",)
          msgs = executeSql.ExecuteAllNew(sql, data)
          return msgs
    except Exception as e:
        print("Error in getCamByid query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1023310') 


# ==============================bigsheet Table End =======================================



# ==================================== AIF Table Start==================================
def InsertAifData(data):
    try:
        sql = " INSERT INTO tbl_aif (entityid, trans_date, trans_type, contribution_amount, setup_expense, stamp_duty, amount_invested, post_tax_nav, num_units, balance_units, strategy_name, amc_name, created_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')
    


def insertNavData(data):
    try:
        sql = " INSERT INTO tbl_aif_nav (entityid, pre_tax_nav, post_tax_nav, nav_date, created_at) VALUES (%s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')    
    

def getAllAif():
     try:
          # sql="SELECT * FROM tbl_entity WHERE category ILIKE 'Equity' AND subcategory ILIKE 'AIF';"
          sql="SELECT * FROM tbl_aif;"

          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
     
def getAifActionTablebyId (entity_id):
    try:
        sql = "SELECT * FROM tbl_aif WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')     


def getAifEntity():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Equity' AND subcategory ILIKE 'Alternative Investment Funds';"
        #   sql="SELECT e.*, a.* FROM tbl_entity e LEFT JOIN tbl_aif a ON e.entityid = a.entityid  WHERE e.category ILIKE 'Equity' AND e.subcategory ILIKE 'Alternative Investment Funds';"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')      



# ==================================== AIF Table End====================================



# ==================================== ETF Table Start==================================
def InsertEtfData(data):
    try:
        sql = " INSERT INTO tbl_etf_action (entityid, order_number, order_time, trade_number, trade_time, security_description, order_type, quantity, gross_rate, trade_price_per_unit, brokerage_per_unit, net_rate_per_unit, closing_rate, gst, stt, net_total_before_levies, remarks, created_at, trade_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')
    

def getAllEtf():
     try:
        #   sql="SELECT * FROM tbl_etf_action;"
        sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_etf_action b ON e.entityid = b.entityid;"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
     
def getEtfActionTablebyId (entity_id):
    try:
        sql = "SELECT * FROM tbl_etf_action WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')     


def getEtfEntity():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Commodities' AND subcategory ILIKE 'ETF';"
        #   sql="SELECT e.*, a.* FROM tbl_entity e LEFT JOIN tbl_aif a ON e.entityid = a.entityid  WHERE e.category ILIKE 'Equity' AND e.subcategory ILIKE 'Alternative Investment Funds';"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')      



# ==================================== ETF Table End====================================





# ====================================Direct table start============================
# def Insert_directData(data):
#     try:
#         sql = " INSERT INTO tbl_direct_equity (entityid, contract_note_number, trade_date, client_code, client_name, order_number, order_time, trade_number, description, order_type, qty, trade_price, brokerage_per_unit, net_rate_per_unit, gst, stt, security_transaction_tax, exchange_transaction_charges, sebi_turnover_fees, stamp_duty, ipft, net_total, net_amount_receivable, created_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"
#         msg = executeSql.ExecuteOne(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in save_user query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')
    
def Insert_directData(data):
    try:
        sql = "INSERT INTO tbl_direct_equity (entityid, contract_note_number, trade_date, client_code, client_name, order_number, order_time, trade_number, description, order_type,qty, trade_price, brokerage_per_unit, net_rate_per_unit, gst, stt, security_transaction_tax, exchange_transaction_charges, sebi_turnover_fees,stamp_duty, ipft, net_total, net_amount_receivable, created_at) VALUES (%s, %s, %s::date, %s, %s, %s, %s::time, %s, %s, %s,%s::int, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric,%s::numeric, %s::numeric, %s::numeric, %s::numeric, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


def getallDirectdata():
     try:
          sql="SELECT * FROM tbl_entity WHERE category = 'Equity' AND subcategory = 'Direct Equity';"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
     
# def getdirectByentId(entity_id):
#     try:
#         sql = "SELECT * FROM tbl_direct_equity WHERE entityid = %s;"
#         data = (entity_id,)  # tuple, not set
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getting underlying by id query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310') 

def getdirectByentId(entity_id):
    try:
        sql = "SELECT * FROM tbl_direct_equity WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310') 


def getAllActionTableOfDirectEquity():
     try:
          sql="SELECT d.* FROM tbl_direct_equity d JOIN tbl_entity e ON e.entityid = d.entityid WHERE e.category ILIKE 'Equity' AND e.subcategory ILIKE 'Direct Equity';"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
     
def getDirectEquityByid(entity_id):
    try:
        sql = "SELECT * FROM tbl_entity WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310') 

def getDEDetailActionTable(entity_id):
    try:
        sql = "SELECT * FROM get_direct_equity_fifo() WHERE entityid = %s ORDER BY trade_date, trade_id;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')           

# ====================================Direct table end============================

# ======================================Delete Etity data From Underlying,Action Table============================
def delete_entity_data(entity_id):
    try:
        deleted_summary = {}

        # Child tables list (table_name : delete_sql)
        delete_queries = {
            "tbl_underlying": "DELETE FROM tbl_underlying WHERE entityid = %s",
            "tbl_action_table": "DELETE FROM tbl_action_table WHERE entityid = %s",
            "tbl_direct_equity": "DELETE FROM tbl_direct_equity WHERE entityid = %s",
            "tbl_etf_action": "DELETE FROM tbl_etf_action WHERE entityid = %s"
            #Add more tables here if needed
        }

        for table, sql in delete_queries.items():
            deleted_count = executeSql.ExecuteReturnId(sql, (entity_id,))
            if isinstance(deleted_count, int):
                deleted_summary[table] = deleted_count
            else:
                deleted_summary[table] = 0  # fallback if not integer

        return deleted_summary

    except Exception as e:
        print("Error in DeleteEntityByid query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')
    
# ======================================Delete Etity data From Underlying,Action Table======================================    


# ======================================Get All Equity======================================
def getAllEquity():
     try:
          sql="SELECT *FROM tbl_entity WHERE LOWER(category) IN ('equity');"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
# ======================================Get All Equity======================================  



# ======================================Get All EquityActionTable======================================
def getEquityActionTable():
     try:
          sql="WITH counts AS (SELECT (SELECT COUNT(*) FROM tbl_action_table) AS action_count,(SELECT COUNT(*) FROM tbl_aif) AS aif_count,(SELECT COUNT(*) FROM tbl_direct_equity) AS equity_count)SELECT action_count,aif_count,equity_count,(action_count * 100.0 / (action_count + aif_count + equity_count))::numeric(5,2) AS action_percent,(aif_count * 100.0 / (action_count + aif_count + equity_count))::numeric(5,2)    AS aif_percent,(equity_count * 100.0 / (action_count + aif_count + equity_count))::numeric(5,2) AS equity_percent FROM counts;"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
# ======================================Get All EquityActionTable======================================


# ======================================Get All Home Data of Equity======================================
def getAllHomeData():
     try:
          sql="WITH instrument_counts AS (SELECT 'Equity' AS instrument, (SELECT COUNT(*) FROM tbl_action_table) + (SELECT COUNT(*) FROM tbl_aif) + (SELECT COUNT(*) FROM tbl_direct_equity) AS instrument_total_count UNION ALL SELECT 'Fixed Income', 0 UNION ALL SELECT 'Commodities', 0)SELECT * FROM instrument_counts;"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310') 
# ======================================Get All Home Data of Equity======================================



#
# ======================================Get All Action  Table Instrument======================================
# def getAllActionInstrument():
#     try:
#         # Fetch action data
#         sql_action = """
#             SELECT COALESCE(order_type, '-') AS order_type,
#                    COALESCE(scrip_name, '-') AS scrip_name
#             FROM tbl_action_table
#         """
#         action_result = executeSql.FetchAll(sql_action)

#         # Fetch AIF data
#         sql_aif = """
#             SELECT COALESCE(amc_name, '-') AS amc_name,
#                    COALESCE(contribution_amount::text, '-') AS contribution_amount
#             FROM tbl_aif
#         """
#         aif_result = executeSql.FetchAll(sql_aif)

#         # Fetch direct equity data
#         sql_direct_equity = """
#             SELECT COALESCE(order_type, '-') AS order_type,
#                    COALESCE(trade_price::text, '-') AS trade_price
#             FROM tbl_direct_equity
#         """
#         direct_equity_result = executeSql.FetchAll(sql_direct_equity)

#         # Always return only the data[] portion
#         return {
#             "action_data": action_result["data"],
#             "aif_data": aif_result["data"],
#             "direct_equity_data": direct_equity_result["data"]
#         }

#     except Exception as e:
#         print("Error in getAllActionInstrument ==============================", e)
#         return {
#             "action_data": [],
#             "aif_data": [],
#             "direct_equity_data": []
#         }

def getAllActionInstrument():
    try:
        action_data = executeSql.FetchAll("SELECT order_type, scrip_name FROM tbl_action_table")
        print("DEBUG action_data:", action_data)

        aif_data = executeSql.FetchAll("SELECT amc_name, contribution_amount FROM tbl_aif")
        print("DEBUG aif_data:", aif_data)

        direct_equity_data = executeSql.FetchAll("SELECT order_type, trade_price FROM tbl_direct_equity")
        print("DEBUG direct_equity_data:", direct_equity_data)

        return {
            "action_data": action_data,
            "aif_data": aif_data,
            "direct_equity_data": direct_equity_data
        }

    except Exception as e:
        print("Error in getAllActionInstrument==============================", e)
        return {"action_data": [], "aif_data": [], "direct_equity_data": []}

# ======================================Get All Action  Table Instrument======================================







# ======================================calculate Xirr (IRR)======================================

# def get_cashflows_action(entityid):
#     sql = """
#         SELECT order_date::date, order_type, purchase_amount, redeem_amount
#         FROM tbl_action_table
#         WHERE TRIM(entityid) ILIKE TRIM(%s)
#         ORDER BY order_date
#     """
#     rows = executeSql.ExecuteAllWithHeaders(sql, (entityid.strip(),))

#     cashflows, dates = [], []

#     for r in rows:
#         if not r.get("order_type"):
#             continue
#         order_type = r["order_type"].lower()
#         if order_type == "purchase":
#             cashflows.append(-float(r.get("purchase_amount") or 0))
#         elif order_type == "sell":
#             cashflows.append(float(r.get("redeem_amount") or 0))
#         else:
#             continue
#         dates.append(r["order_date"])

#     return cashflows, dates

def get_cashflows_action(entityid):
    sql = "SELECT order_date::date, TRIM(LOWER(order_type)) AS order_type,purchase_amount, redeem_amount FROM tbl_action_table WHERE TRIM(entityid) ILIKE %s ORDER BY order_date;"
    rows = executeSql.ExecuteAllWithHeaders(sql, (entityid,))

    cashflows, dates = [], []
    for r in rows:
        order_type = (r.get("order_type") or "").strip().lower()

        if order_type in ("purchase", "buy"):
            amt = float(r.get("purchase_amount") or 0)
            if amt > 0:
                cashflows.append(-amt)
                dates.append(r["order_date"])

        elif order_type in ("sell", "redeem", "redemption"):
            # first try redeem_amount, but if it's 0 and purchase_amount > 0, use that
            amt = float(r.get("redeem_amount") or 0)
            if amt <= 0:
                amt = float(r.get("purchase_amount") or 0)
            if amt > 0:
                cashflows.append(amt)
                dates.append(r["order_date"])

    return cashflows, dates

# def get_cashflows_direct_equity(entityid):
#     sql = "SELECT trade_date::date, order_type, net_total, net_amount_receivable FROM tbl_direct_equity WHERE TRIM(entityid) ILIKE %s ORDER BY trade_date;"
#     rows = executeSql.ExecuteAllWithHeaders(sql, (entityid.strip(),))

#     cashflows, dates = [], []

#     for r in rows:
#         transaction_type = (r.get("order_type") or "").strip().lower()

#         if transaction_type == "buy":
#             amt = float(r.get("net_total") or 0)
#             if amt > 0:
#                 cashflows.append(-amt)
#                 dates.append(r["trade_date"])

#         elif transaction_type == "sell":
#             amt = float(r.get("net_amount_receivable") or 0)
#             if amt > 0:
#                 cashflows.append(amt)
#                 dates.append(r["trade_date"])

#     return cashflows, dates

def get_cashflows_direct_equity(entityid):
    sql = """
        SELECT trade_date::date, order_type, net_total, net_amount_receivable
        FROM tbl_direct_equity
        WHERE TRIM(entityid) ILIKE %s
        ORDER BY trade_date;
    """
    rows = executeSql.ExecuteAllWithHeaders(sql, (entityid.strip(),))
    cashflows, dates = [], []

    for r in rows:
        order_type = (r.get("order_type") or "").strip().lower()

        if order_type == "buy":
            amt = float(r.get("net_total") or 0)
            if amt > 0:
                cashflows.append(-amt)       # Outflow
                dates.append(r["trade_date"])

        elif order_type == "sell":
            amt = float(r.get("net_amount_receivable") or 0)
            if amt > 0:
                cashflows.append(amt)        # Inflow
                dates.append(r["trade_date"])

    return cashflows, dates




def get_cashflows_aif(entityid):
    sql = """
        SELECT trans_date::date, trans_type, 
               contribution_amount, amount_invested, 
               setup_expense, stamp_duty
        FROM tbl_aif
        WHERE TRIM(entityid) ILIKE %s
        ORDER BY trans_date
    """
    rows = executeSql.ExecuteAllWithHeaders(sql, (entityid,))
    cashflows, dates = [], []

    for r in rows:
        ttype = (r["trans_type"] or "").lower()

        if ttype == "subscription":
            # Treat subscription as cash outflow
            invested = float(r.get("amount_invested") or 0)
            cashflows.append(-invested)
        elif ttype == "distribution":
            # Treat distribution as inflow
            # if you have distribution_amount column, use it;
            # otherwise assume "contribution_amount" is 0 and NAV payout is external
            distributed = float(r.get("post_tax_nav") or 0)
            cashflows.append(distributed)
        else:
            continue

        dates.append(r["trans_date"])

    return cashflows, dates

# ======================================calculate Xirr (IRR)======================================




# //////////////////////////TESTING///////////////////////////////////

def get_distinct_entityids_action():
    sql = "SELECT DISTINCT TRIM(entityid) AS entityid FROM tbl_action_table ORDER BY entityid"
    return executeSql.ExecuteAllWithHeaders(sql)

def get_distinct_entityids_equity():
    sql = "SELECT DISTINCT TRIM(entityid) AS entityid FROM tbl_direct_equity ORDER BY entityid"
    return executeSql.ExecuteAllWithHeaders(sql)

def get_distinct_entityids_aif():
    sql = "SELECT DISTINCT TRIM(entityid) AS entityid FROM tbl_aif ORDER BY entityid"
    return executeSql.ExecuteAllWithHeaders(sql)




































   






     

     

     

     



     
     

   
              

      


            

     

     

     




     
     

     

     
 
     
  
     
  
     

   

 

 

 
# end    
           