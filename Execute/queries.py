from datetime import datetime
from Execute import executeSql,responses,middleware
import platform
from flask import jsonify, request
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
#         sql = "DELETE FROM tbl_underlying WHERE id = %s"
#         data = (entity_id,) 
#         deleted_count = executeSql.ExecuteReturnId(sql, data) 
#         return deleted_count
#     except Exception as e:
#         print("Error in DeleteUnderlyingByid query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')  
# 
# 
# def ClearUnderlyingdata(entity_id):
#     try:
#         deleted_summary = {}

#         # Child tables list (table_name : delete_sql)
#         delete_queries = {
#             "tbl_underlying": "DELETE FROM tbl_underlying WHERE entityid = %s",
#         }

#         for table, sql in delete_queries.items():
#             deleted_count = executeSql.ExecuteReturnId(sql, (entity_id,))
#             if isinstance(deleted_count, int):
#                 deleted_summary[table] = deleted_count
#             else:
#                 deleted_summary[table] = 0  # fallback if not integer

#         return deleted_summary

#     except Exception as e:
#         print("Error in DeleteEntityByid query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')     


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

def ClearUnderlyingdata(entity_id):
    try:
        result_summary = {}

        # 1. Check if entityid exists in tbl_underlying
        check_underlying_sql = "SELECT 1 FROM tbl_underlying WHERE entityid = %s"
        underlying_exists = executeSql.ExecuteReturn(check_underlying_sql, (entity_id,))

        if underlying_exists:
            # Case A: entityid exists in tbl_underlying → delete it
            delete_sql = "DELETE FROM tbl_underlying WHERE entityid = %s"
            executeSql.ExecuteOne(delete_sql, (entity_id,))
            result_summary["action"] = "deleted"
            result_summary["rows_affected"] = 1

        else:
            # Case B: entityid not in tbl_underlying → check if it exists in tbl_entity
            check_entity_sql = "SELECT 1 FROM tbl_entity WHERE entityid = %s"
            entity_exists = executeSql.ExecuteReturn(check_entity_sql, (entity_id,))

            if entity_exists:
                # Copy matching fields from tbl_entity → tbl_underlying
                insert_sql = """
                    INSERT INTO tbl_underlying 
                        (company_name, scripcode, weightage, sector, isin_code, created_at, entityid)
                    SELECT 
                        scripname,       -- maps to company_name
                        scripcode,       -- maps to scripcode
                        weightage,       -- maps to weightage
                        sector,          -- maps to sector
                        isin,            -- maps to isin_code
                        NOW(),           -- maps to created_at
                        entityid         -- maps to entityid
                    FROM tbl_entity
                    WHERE entityid = %s
                """
                executeSql.ExecuteOne(insert_sql, (entity_id,))
                result_summary["action"] = "inserted"
                result_summary["rows_affected"] = 1
            else:
                # Case C: entityid not in tbl_entity either → nothing to do
                result_summary["action"] = "not_found"
                result_summary["rows_affected"] = 0

        return result_summary

    except Exception as e:
        print("Error in ClearUnderlyingdata query:", e)
        return {
            "action": "error",
            "error": str(e),
            "rows_affected": 0
        }

# ####

# ==============================Underlying Table End =======================================

     

# ==============================bigsheet Table Start =======================================   
    
# def getCamByid():
#      try:
#           sql="SELECT * FROM tbl_underlying"
#           data=''
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingroleRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
   


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
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')     


def getAifEntity():
     try:
          # sql="SELECT e.* FROM tbl_aif a JOIN tbl_entity e ON e.entityid = a.entityid WHERE e.category ILIKE 'Equity' AND e.subcategory ILIKE 'Alternative Investment Funds';"
          sql="SELECT e.*, a.* FROM tbl_entity e LEFT JOIN tbl_aif a ON e.entityid = a.entityid  WHERE e.category ILIKE 'Equity' AND e.subcategory ILIKE 'Alternative Investment Funds';"

          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')      



# ==================================== AIF Table End====================================



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
        sql = "SELECT * FROM tbl_direct_equity  WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310') 

def getDEDetailActionTable(entity_id):
    try:
        sql = "SELECT * FROM tbl_direct_equity  WHERE entityid = %s;"
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
            "tbl_action_table": "DELETE FROM tbl_action_table WHERE entityid = %s"
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
#     try:
#         sql = """
#             SELECT order_date, purchase_amount 
#             FROM tbl_action_table 
#             WHERE entityid = %s 
#             ORDER BY order_date;
#         """
#         data = (entityid,)
#         msgs = executeSql.ExecuteAllNew(sql, data)

#         if not msgs:   # no rows found
#             return [], []

#         dates = [row[0] for row in msgs]
#         cashflows = [-float(row[1]) for row in msgs]  # purchases as negative
#         return cashflows, dates

#     except Exception as e:
#         print("Error in get_cashflows_action =================", e)
#         return [], []


def get_cashflows_action(entityid):
    sql = "SELECT CASE WHEN pg_typeof(order_date)::text IN ('date','timestamp','timestamptz') THEN order_date::date ELSE TO_DATE(order_date, 'MM/DD/YYYY') END AS order_date, CASE WHEN pg_typeof(purchase_amount)::text IN ('numeric','integer','double precision')THEN purchase_amount::numeric ELSE NULLIF(REGEXP_REPLACE(purchase_amount, '[^0-9.-]', '', 'g'), '')::numeric END AS purchase_amount FROM tbl_action_table WHERE TRIM(entityid) = %s ORDER BY order_date;"
    rows = executeSql.ExecuteAllNew(sql, (entityid,))

    print("DEBUG rows for", entityid, "=>", type(rows), rows)

    if not rows or not isinstance(rows, list):
        raise ValueError(f"No rows returned from DB for entityid={entityid}")

    cashflows, dates = [], []

    for row in rows:  # row is a dict
        order_date = row["order_date"]
        purchase_amount = float(row["purchase_amount"] or 0)

        if purchase_amount != 0:
            cashflows.append(-purchase_amount)
            dates.append(order_date)

    if not cashflows:
        raise ValueError("No cashflows found after processing rows")

    return cashflows, dates




# def get_cashflows_aif(entityid):
#     sql = "SELECT trans_date, contribution_amount FROM tbl_aif WHERE entityid = %s ORDER BY trans_date;"
#     data = (entityid,)
#     msgs = executeSql.ExecuteAllNew(sql, data)
#     dates = [row[0] for row in msgs]
#     cashflows = [float(row[1]) for row in msgs]
#     return cashflows, dates


# def get_cashflows_equity(entityid):
#     sql = "SELECT trade_date, trade_price FROM tbl_direct_equity WHERE entityid = %s ORDER BY trade_date;"
#     data = (entityid,)
#     msgs = executeSql.ExecuteAllNew(sql, data)
#     dates = [row[0] for row in msgs]
#     cashflows = [float(row[1]) for row in msgs]
#     return cashflows, dates


# ======================================calculate Xirr (IRR)======================================











































   






     

     

     

     



     
     

   
              

      


            

     

     

     




     
     

     

     
 
     
  
     
  
     

   

 

 

 
# end    
           