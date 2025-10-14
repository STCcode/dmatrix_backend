from datetime import datetime
from Execute import executeSql,responses,middleware
import platform
import psycopg2
import pandas as pd
from flask import session,jsonify, request
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

# old
# def login_user(data):
#     try:
#         sql = "SELECT id, name, email FROM tbl_users WHERE email = %s AND password = md5(%s)"
#         return executeSql.ExecuteAllNew(sql, data)
#     except Exception as e:
#         print("Error in login_user query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020320')
# old

def login_user(data):
    try:
        sql = "SELECT id, name, email, password, role FROM tbl_users WHERE email = %s"
        return executeSql.ExecuteAllNew(sql, data)
    except Exception as e:
        print("Error in login_user query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020320')


# postgres query
# def save_user(data):
#     try:
#      #    sql = " INSERT INTO tbl_users (name, email, password, created_by, created_date, updated_date) VALUES (%s, %s, md5(%s), %s, %s, %s)"
#         sql = " INSERT INTO tbl_users (name, email, password, created_date, updated_date) VALUES (%s, %s, md5(%s), %s, %s)"
#         msg = executeSql.ExecuteReturnId(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in save_user query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

def save_user(data):
    try:
        sql = " INSERT INTO tbl_users (name, email, password, role, created_by, created_date, updated_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query:", e)
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

# def getAlluser():
#      try:
#           sql="SELECT *,getrolename(s_role) as role_name FROM tbl_user_master"
#           data=''
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingroleRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# def get_all_users():
#     try:
#         sql = "SELECT id, name, email, role, created_date FROM tbl_users ORDER BY id DESC"
#         return executeSql.ExecuteAllNew(sql)
#     except Exception as e:
#         print("Error in get_all_users query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020330')
def get_all_users():
    try:
        sql = "SELECT id, name, email, role, created_date FROM tbl_users ORDER BY id DESC"
        data = executeSql.ExecuteAllNew(sql)

        # Ensure result is a list, not Response
        if not isinstance(data, list):
            print(" Warning: get_all_users() returned non-list:", type(data))
            return []

        return data

    except Exception as e:
        print("Error in get_all_users query:", e)
        return []


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
        sql = "INSERT INTO tbl_entity (scripname, scripcode, entityID, benchmark_name, category,subcategory, nickname, created_at, isin,aif_category, aif_class,created_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


# def getAllentity():
#      try:
          
#         role = session.get('role')
#         email = session.get('email')

#         if role =='admin':
#           sql="SELECT * FROM tbl_entity"
#           data=''

#         else:
#             sql = "SELECT * FROM tbl_entity WHERE created_by = %s ORDER BY id DESC"
#             # data = (email,)
#             data = (session['email'],)
#         msgs=executeSql.ExecuteAllNew(sql,data)
#         return msgs
     
#      except Exception as e:
#           print("Error in getingroleRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getAllentity():
    try:
        # Get role and email from session
        # role = session.get('role')
        # email = session.get('email')

        # Check if user is logged in
        # if not role or not email:
        #     return middleware.exe_msgs(responses.queryError_501, "Unauthorized: No active session", '1023311')

        # Admin can see all entities
        # if role == 'admin':
        sql = "SELECT * FROM tbl_entity ORDER BY id DESC"
        data = ''

        # Normal users only see their own
        # else:
        #     sql = "SELECT * FROM tbl_entity WHERE created_by = %s ORDER BY id DESC"
        #     data = (email,)

        print(f"[DEBUG] Running query: {sql} with {data}")  # optional debug line

        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs

    except Exception as e:
        print("Error in getAllentity query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1023310')



def getAllMutualFund():
    try:
        # role = session.get('role')
        # email = session.get('email')

        # if not role or not email:
        #       return middleware.exe_msgs(responses.queryError_501,"Unauthorized: No Active session", '1023311')
          
        # if role =='admin':
        sql="SELECT * FROM tbl_entity WHERE category = 'Equity' AND subcategory = 'Mutual Fund';"

        data = ''
        # else:
        #      sql = "select * From tbl_entity WHERE category ='Equit' AND subcategory ='Mutual Fund' AND created_by = %s;"
        #      data = (email,)

        # print(f"[DEBUG] Running query: {sql} with {data}")  # optional debug line
          
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
    except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getMutualFundDataById(entity_id):
    try:
        # role = session.get('role')
        # email = session.get('email')

        # if not role or not email:
        #     return middleware.exe_msgs(responses.queryError_501,"Unauthorized: No Active session",'1023311')
        # if role == 'admin':
        sql = "SELECT * FROM tbl_entity  WHERE entityid = %s;"
        data = (entity_id,)

        # else:
        #     sql ='SELECT * FROM tbl_entity WHERE entityid = %s AND created_by = %s;'  
        #     data = (entity_id, email)  

        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

def update_entity_table(data):
    try:
        sql = "UPDATE tbl_entity SET scripname = %s,scripcode = %s,benchmark_name = %s,category = %s,subcategory = %s,nickname = %s,isin =%s,aif_category =%s, aif_class =%s,updated_at = %s WHERE id = %s"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in update_entity_table query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020311')

# def DeleteEntityByid(entity_id):
#     try:
#         entity_id = int(entity_id)
#         sql = "DELETE FROM tbl_entity WHERE id = %s"
#         data = (entity_id,)
#         deleted_count = executeSql.ExecuteReturnId(sql, data)
#         return deleted_count
#     except Exception as e:
#         print("Error in DeleteEntityByid query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')

def DeleteEntityByid(entity_id):
    try:
        entity_id = int(entity_id)
        sql = "DELETE FROM tbl_entity WHERE id = %s RETURNING id"
        data = (entity_id,)
        deleted_count = executeSql.ExecuteReturnId(sql, data)
        return deleted_count
    except Exception as e:
        print("Error in DeleteEntityByid query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')



def getCountOfAllEntity():
     try:
        # role = session.get('role')
        # email = session.get('email')

        # if not role or not email:
        #     return middleware.exe_msgs(responses.queryError_501,"Unauthorized: No Active session",'1023311')

        # if role == 'admin':
        sql=" SELECT subcategory,category,COUNT(*) AS total FROM tbl_entity WHERE category ILIKE 'Equity' GROUP BY subcategory, category ORDER BY subcategory;"
        data=''
        # else:
        #   sql ="SELECT subcategory, category, COUNT(*) AS total FROM    tbl_entity where category ILIKE 'Equity' GROUPE BY subcategory, category ORDER by subcategory" 
        #   data=(email,)   

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
        sql = " INSERT INTO tbl_action_table (scrip_code, mode, order_type, scrip_name, isin, order_number, folio_number, nav, stt, unit, redeem_amount, purchase_amount, cgst, sgst, igst, ugst, stamp_duty, cess_value,net_amount,created_at,entityid,purchase_value,order_date,sett_no, created_by) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


def getAllAction():
    try:
     
        # role = session.get('role')
        # email = session.get('email')

        # if not role or not email:
        #     return middleware.exe_msgs(responses.queryError_501, "Unauthorized: No active session", '1023311')

        # if role =='admin':
        sql="SELECT * FROM tbl_action_table"
        data=''
        # else:
        #     sql = "SELECT * FROM tbl_action_table WHERE created_by = %s ORDER BY id DESC"
        #     data = (email,)  
        # print(f"[DEBUG] Running query: {sql} with {data}")  # optional debug line


        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
    except Exception as e:
        print("Error in getingroleRecord query==========================",e)
        return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def updateMFDetailActionTableRow(data, record_id):
    try:
        sql = "UPDATE tbl_action_table SET scrip_code = %s, mode = %s, order_type = %s, scrip_name = %s, isin = %s, order_number = %s, folio_number = %s, nav = %s, stt = %s, unit = %s, redeem_amount = %s, purchase_amount = %s, cgst = %s, sgst = %s,igst = %s,ugst = %s,stamp_duty = %s,cess_value = %s,net_amount = %s,entityid = %s,purchase_value = %s,order_date = %s,sett_no = %s,updated_at = NOW() WHERE id = %s"

        # Append record_id at the end
        final_data = list(data) + [record_id]

        # Execute SQL
        rows_affected = executeSql.ExecuteReturnId(sql, final_data)
        return rows_affected

    except Exception as e:
        print("Error in updateMFDetailActionTableRow query ==========================", e)
        return middleware.exe_msgs(responses.update_501, str(e.args), '1020501')

def deleteMFDetailActionTableRow(entity_id):
    try:
        entity_id = int(entity_id)
        sql = "DELETE FROM tbl_action_table WHERE id = %s RETURNING id"
        data = (entity_id,)
        deleted_count = executeSql.ExecuteReturnId(sql, data)
        return deleted_count
    except Exception as e:
        print("Error in DeleteEntityByid query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')


def getActionByentId(entity_id):
    try:
        # role = session.get('role')
        # email = session.get('email')
        # sql = "SELECT * FROM tbl_action_table WHERE entityid = %s;"
        # sql ="SELECT COALESCE(redeem_amount::text, '-') AS redeem_amount,COALESCE(purchase_amount::text, '-') AS purchase_amount,COALESCE(purchase_value::text, '-') AS purchase_value FROM tbl_action_table WHERE entityid = %s;"
        # if not role or not email:
        #     return middleware.exe_msgs(responses.queryError_501, "Unauthorized: No active session", '1023311')


        # if role == 'admin':
        sql="SELECT id::text,COALESCE(scrip_code::text, '-') AS scrip_code,COALESCE(mode, '-') AS mode,COALESCE(order_type, '-') AS order_type,COALESCE(scrip_name, '-') AS scrip_name,COALESCE(isin::text, '-') AS isin,COALESCE(order_number::text, '-') AS order_number,COALESCE(folio_number::text, '-') AS folio_number,COALESCE(nav::text, '-') AS nav,COALESCE(stt::text, '-') AS stt,COALESCE(unit::text, '-') AS unit,COALESCE(redeem_amount::text, '-') AS redeem_amount,COALESCE(purchase_amount::text, '-') AS purchase_amount,COALESCE(purchase_value::text, '-') AS purchase_value,COALESCE(cgst::text, '-') AS cgst,COALESCE(sgst::text, '-') AS sgst,COALESCE(igst::text, '-') AS igst,COALESCE(ugst::text, '-') AS ugst,COALESCE(stamp_duty::text, '-') AS stamp_duty,COALESCE(cess_value::text, '-') AS cess_value,COALESCE(net_amount::text, '-') AS net_amount,COALESCE(created_at::text, '-') AS created_at,COALESCE(entityid, '-') AS entityid,COALESCE(order_date::text, '-') AS order_date,COALESCE(sett_no::text, '-') AS sett_no,COALESCE(updated_at::text, '-') AS updated_at FROM tbl_action_table WHERE entityid = %s;"
        data = (entity_id,)
        # else:
        #     sql="SELECT id::text,COALESCE(scrip_code::text, '-') AS scrip_code,COALESCE(mode, '-') AS mode,COALESCE(order_type, '-') AS order_type,COALESCE(scrip_name, '-') AS scrip_name,COALESCE(isin::text, '-') AS isin,COALESCE(order_number::text, '-') AS order_number,COALESCE(folio_number::text, '-') AS folio_number,COALESCE(nav::text, '-') AS nav,COALESCE(stt::text, '-') AS stt,COALESCE(unit::text, '-') AS unit,COALESCE(redeem_amount::text, '-') AS redeem_amount,COALESCE(purchase_amount::text, '-') AS purchase_amount,COALESCE(purchase_value::text, '-') AS purchase_value,COALESCE(cgst::text, '-') AS cgst,COALESCE(sgst::text, '-') AS sgst,COALESCE(igst::text, '-') AS igst,COALESCE(ugst::text, '-') AS ugst,COALESCE(stamp_duty::text, '-') AS stamp_duty,COALESCE(cess_value::text, '-') AS cess_value,COALESCE(net_amount::text, '-') AS net_amount,COALESCE(created_at::text, '-') AS created_at,COALESCE(entityid, '-') AS entityid,COALESCE(order_date::text, '-') AS order_date,COALESCE(sett_no::text, '-') AS sett_no,COALESCE(updated_at::text, '-') AS updated_at FROM tbl_action_table WHERE entityid = %s AND created_by = %s;"    
        # data = (entity_id, email)  # tuple, not set
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

def insert_MF_NavData(data):
    """
    Always insert a new NAV entry for an ISIN — no conflict handling.
    """
    try:
        sql = """
            INSERT INTO tbl_mutual_fund_nav (isin, nav, nav_date)
            VALUES (%s, %s, %s)
        """
        success = executeSql.ExecuteOne(sql, data)
        return success > 0
    except Exception as e:
        print("Error in insert_MF_NavData:", e)
        return False







def getAll_Mutual_Fund_Nav(isin):
     try:
        # role = session.get('role')
        # email = session.get('email')
        #   sql="SELECT * FROM tbl_mutual_fund_nav"
        # if role == 'admin':
        sql="SELECT * FROM tbl_mutual_fund_nav where isin= %s order by nav_date ASC;"
        data=(isin,)
        # else:
        #     sql="SELECT * FROM tbl_mutual_fund_nav where isin= %s AND create_by = %s ORDER by nav_date ASC;"
        #     data=(isin, email)
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getAllMFEquitytotalValue():
    try:
        # role = session.get('role')
        # email = session.get('email')

        # if role == 'admin':
        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin) isin, nav FROM tbl_mutual_fund_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT a.entityid, SUM(a.unit * nav.nav) AS entity_total_value FROM tbl_action_table a JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY a.entityid),total_value AS ( SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values), entity_mf_percent AS ( SELECT ev.entityid,ev.entity_total_value, (ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv), entity_weights AS ( SELECT u.entityid, u.tag, COUNT(*) AS tag_count, SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.tag IS NOT NULL GROUP BY u.entityid, u.tag), entity_totals AS ( SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid ), entity_tag_breakdown AS ( SELECT ew.entityid, ew.tag, ew.tag_count, ew.tag_weight, et.total_tag_weight, ev.entity_total_value,emp.mf_weight_percent, (ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction, ROUND((ew.tag_weight / et.total_tag_weight) * emp.mf_weight_percent, 2) AS total_tag_percent FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_values ev ON ew.entityid = ev.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid), tag_totals AS ( SELECT tag, ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag) SELECT etb.entityid, etb.tag, etb.tag_count, etb.tag_weight, etb.total_tag_weight, etb.entity_total_value, etb.mf_weight_percent, etb.total_tag_percent, CASE  WHEN ROW_NUMBER() OVER (PARTITION BY etb.tag ORDER BY etb.entityid) = 1 THEN tt.overall_tag_percent END AS overall_tag_percent FROM entity_tag_breakdown etb JOIN tag_totals tt ON etb.tag = tt.tag ORDER BY etb.tag, etb.entityid;"
        # data = (entity_id,)  # tuple, not set
        
        data = ''
        # else:
        #     sql = "WITH latest_nav AS (SELECT DISTINCT ON (isin) isin, nav FROM tbl_mutual_fund_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT a.entityid, SUM(a.unit * nav.nav) AS entity_total_value FROM tbl_action_table a JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND e.created_by = %s GROUP BY a.entityid),total_value AS ( SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values), entity_mf_percent AS ( SELECT ev.entityid,ev.entity_total_value, (ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv), entity_weights AS ( SELECT u.entityid, u.tag, COUNT(*) AS tag_count, SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND e.created_by = %s AND u.tag IS NOT NULL GROUP BY u.entityid, u.tag), entity_totals AS ( SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid ), entity_tag_breakdown AS ( SELECT ew.entityid, ew.tag, ew.tag_count, ew.tag_weight, et.total_tag_weight, ev.entity_total_value,emp.mf_weight_percent, (ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction, ROUND((ew.tag_weight / et.total_tag_weight) * emp.mf_weight_percent, 2) AS total_tag_percent FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_values ev ON ew.entityid = ev.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid), tag_totals AS ( SELECT tag, ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag) SELECT etb.entityid, etb.tag, etb.tag_count, etb.tag_weight, etb.total_tag_weight, etb.entity_total_value, etb.mf_weight_percent, etb.total_tag_percent, CASE  WHEN ROW_NUMBER() OVER (PARTITION BY etb.tag ORDER BY etb.entityid) = 1 THEN tt.overall_tag_percent END AS overall_tag_percent FROM entity_tag_breakdown etb JOIN tag_totals tt ON etb.tag = tt.tag ORDER BY etb.tag, etb.entityid;"
        #     data = (email,)
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')



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
#             rows_deleted = executeSql.ExecuteOne(delete_sql, (entity_id,))
#             result_summary["action"] = "deleted"
#             result_summary["rows_affected"] = rows_deleted

#         else:
#             # Case B: entityid not in tbl_underlying → check if it exists in tbl_entity
#             check_entity_sql = "SELECT 1 FROM tbl_entity WHERE entityid = %s"
#             entity_exists = executeSql.ExecuteReturn(check_entity_sql, (entity_id,))

#             if entity_exists:
#                 # Insert entityid into tbl_underlying
#                 insert_sql = "INSERT INTO tbl_underlying (entityid) VALUES (%s)"
#                 executeSql.ExecuteReturnId(insert_sql, (entity_id,))
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


def ClearUnderlyingdata(entity_id):
    try:
        result_summary = {}

        # 1. Check if entityid exists in tbl_underlying
        check_underlying_sql = "SELECT 1 FROM tbl_underlying WHERE entityid = %s"
        underlying_exists = executeSql.ExecuteReturn(check_underlying_sql, (entity_id,))

        if underlying_exists:
            # Case A: entityid exists in tbl_underlying → delete it
            delete_sql = "DELETE FROM tbl_underlying WHERE entityid = %s"
            rows_deleted = executeSql.ExecuteOne(delete_sql, (entity_id,))
            result_summary["action"] = "deleted"
            result_summary["rows_affected"] = rows_deleted

        else:
            # Case B: entityid not in tbl_underlying → check if it exists in tbl_entity
            check_entity_sql = "SELECT 1 FROM tbl_entity WHERE entityid = %s"
            entity_exists = executeSql.ExecuteReturn(check_entity_sql, (entity_id,))

            if entity_exists:
                # Insert entityid into tbl_underlying
                insert_sql = "INSERT INTO tbl_underlying (entityid) VALUES (%s)"
                rows_inserted = executeSql.ExecuteReturnId(insert_sql, (entity_id,))
                result_summary["action"] = "inserted"
                result_summary["rows_affected"] = rows_inserted
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



# old
# def ClearUnderlyingdata(entity_id: str):
#     try:
#         entity_id = entity_id.strip()  # remove hidden spaces

#         # 1️⃣ Delete rows in tbl_underlying
#         delete_sql = "DELETE FROM tbl_underlying WHERE TRIM(entityid) = TRIM(%s)"
#         rows_deleted = executeSql.ExecuteOne(delete_sql, (entity_id,), return_rowcount=True)

#         if rows_deleted > 0:
#             return {
#                 "action": "deleted",
#                 "rows_affected": rows_deleted
#             }

#         # 2️⃣ Check if entity exists in tbl_entity
#         check_sql = "SELECT 1 FROM tbl_entity WHERE TRIM(entityid) = TRIM(%s)"
#         exists = executeSql.ExecuteOne(check_sql, (entity_id,), return_rowcount=False)

#         if exists:
#             # 3️⃣ Insert entityid if not deleted
#             insert_sql = "INSERT INTO tbl_underlying (entityid) VALUES (%s)"
#             rows_inserted = executeSql.ExecuteOne(insert_sql, (entity_id,), return_rowcount=True)
#             return {
#                 "action": "inserted",
#                 "rows_affected": rows_inserted
#             }
#         else:
#             return {
#                 "action": "not_found",
#                 "rows_affected": 0
#             }

#     except Exception as e:
#         import traceback
#         error_details = traceback.format_exc()
#         print("🔥 ClearUnderlyingdata failed:", error_details)
#         return {
#             "action": "error",
#             "error": str(e),
#             "rows_affected": 0
#         }
# old
# ####

# ==============================Underlying Table End =======================================



# ==============================bigsheet Table Start =======================================


# def getCamByid(company_name=None):
#     try:

#           # sql = "SELECT issuer_name,name_of_company, isin,sector_name,tag FROM equity_bigsheet_data WHERE name_of_company ILIKE %s;"
#         sql = "SELECT DISTINCT ON (company_name) CASE WHEN issuer_name IS NOT NULL AND name_of_company IS NOT NULL AND normalize_company_name(issuer_name) <> normalize_company_name(name_of_company) THEN issuer_name || ' / ' || name_of_company ELSE COALESCE(issuer_name, name_of_company) END AS company_name,isin,sector_name,tag FROM equity_bigsheet_data WHERE normalize_company_name(COALESCE(name_of_company, issuer_name)) ILIKE normalize_company_name(%s) ORDER BY company_name;"
#         # sql="WITH input_name AS (SELECT normalize_company_name(%s) AS search_name) SELECT DISTINCT ON (company_name)CASE WHEN normalize_company_name(issuer_name) IS NOT NULL AND normalize_company_name(name_of_company) IS NOT NULL AND normalize_company_name(issuer_name) <> normalize_company_name(name_of_company)THEN issuer_name || ' / ' || name_of_company WHEN normalize_company_name(issuer_name) IS NOT NULL THEN issuer_name ELSE name_of_company END AS company_name,isin,sector_name,tag FROM equity_bigsheet_data, input_name WHERE normalize_company_name(issuer_name) ILIKE '%' || input_name.search_name || '%'OR normalize_company_name(name_of_company) ILIKE '%' || input_name.search_name || '%' ORDER BY company_name;"
#         data = (f{%company_name%,})
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getCamByid query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1023310')

def getCamByid(company_name=None):
    try:
        company_name = str(company_name or '').strip()

        sql = """
        SELECT DISTINCT ON (company_name)
            CASE
                WHEN issuer_name IS NOT NULL
                     AND issuer_name NOT ILIKE 'NA'
                     AND name_of_company IS NOT NULL
                     AND name_of_company NOT ILIKE 'NA'
                     AND normalize_company_name(issuer_name) <> normalize_company_name(name_of_company)
                THEN issuer_name || ' / ' || name_of_company
                WHEN issuer_name IS NOT NULL AND issuer_name NOT ILIKE 'NA'
                THEN issuer_name
                ELSE NULLIF(name_of_company, 'NA')
            END AS company_name,
            isin,
            sector_name,
            tag
        FROM equity_bigsheet_data
        WHERE normalize_company_name(COALESCE(name_of_company, issuer_name))
              ILIKE normalize_company_name(%s)
        ORDER BY company_name;
        """

        # ✅ add wildcards
        data = (f"%{company_name}%",)

        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs

    except Exception as e:
        print("Error in getCamByid query ====================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1023310')

# ==============================bigsheet Table End =======================================



# ==================================== AIF Table Start==================================
def InsertAifData(data):
    try:
        sql = " INSERT INTO tbl_aif (entityid, trans_date, trans_type, contribution_amount, setup_expense, stamp_duty, amount_invested, post_tax_nav, num_units, balance_units, strategy_name, amc_name, created_at,isin) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')



def insertNavData(data):
    try:
        sql = " INSERT INTO tbl_aif_nav (entityid, pre_tax_nav, post_tax_nav, nav_date, created_at,isin) VALUES (%s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


def updateAIFDetailActionTableRow(data):
    try:
        sql = "UPDATE tbl_aif SET trans_date = %s,trans_type = %s,contribution_amount = %s,setup_expense = %s,stamp_duty = %s,amount_invested = %s,post_tax_nav =%s,num_units =%s,balance_units =%s,strategy_name=%s,amc_name=%s,isin=%s,updated_at = %s WHERE aif_id = %s"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in update_entity_table query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020311')

def deleteAIFDetailActionTableRow(entity_id):
    try:
        entity_id = int(entity_id)
        sql = "DELETE FROM tbl_aif WHERE aif_id = %s RETURNING aif_id"
        data = (entity_id,)
        deleted_count = executeSql.ExecuteReturnId(sql, data)
        return deleted_count
    except Exception as e:
        print("Error in DeleteEntityByid query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')


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


# def getAIFEquityDetailsById(entity_id):
#     try:
#         sql = "SELECT * FROM tbl_entity  WHERE entityid = %s;"
#         data = (entity_id,)  # tuple, not set
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getting underlying by id query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')



# ==================================== AIF Table End====================================


# ==================================== AIF Fixed Income Table Start==================================

def updateAIFFixIncomeDetailActionTableRow(data):
    try:
        sql = "UPDATE tbl_aif SET trans_date = %s,trans_type = %s,contribution_amount = %s,setup_expense = %s,stamp_duty = %s,amount_invested = %s,post_tax_nav =%s,num_units =%s,balance_units =%s,strategy_name=%s,amc_name=%s,isin=%s,updated_at = %s WHERE aif_id = %s"
        msg = executeSql.ExecuteReturnId(sql, data)
        return msg
    except Exception as e:
        print("Error in update_entity_table query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020311')

def deleteAIFFixIncomeDetailActionTableRow(entity_id):
    try:
        entity_id = int(entity_id)
        sql = "DELETE FROM tbl_aif WHERE aif_id = %s RETURNING aif_id"
        data = (entity_id,)
        deleted_count = executeSql.ExecuteReturnId(sql, data)
        return deleted_count
    except Exception as e:
        print("Error in DeleteEntityByid query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1024310')


def getAllAifFixedIncomeActionTable():
     try:
          sql=" SELECT e.scripname, b.* FROM tbl_entity e join tbl_aif b ON e.entityid = b.entityid where e.category ILIKE 'Fixed_Income';"

          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getAIFFixedIncomeDetailsById (entity_id):
    try:
        sql = "SELECT * FROM tbl_aif WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


def getAllAifFixedIncomeEntity():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Fixed_Income' AND subcategory ILIKE 'Alternative Investment Funds';"
        #   sql="SELECT e.*, a.* FROM tbl_entity e LEFT JOIN tbl_aif a ON e.entityid = a.entityid  WHERE e.category ILIKE 'Equity' AND e.subcategory ILIKE 'Alternative Investment Funds';"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
     
     
def getAifDetailsFixedIncomeUnderlyingTable(entity_id):
    try:
        sql = "SELECT u.*, e.scripname FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

# def getAIFFixIncomeDetailsById(entity_id):
#     try:
#         sql = "SELECT * FROM tbl_entity  WHERE entityid = %s;"
#         data = (entity_id,)  # tuple, not set
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getting underlying by id query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')



# ==================================== AIF Fixed Income Table End====================================



# ====================================Commodities ETF Table Start==================================
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
        # sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_etf_action b ON e.entityid = b.entityid;"
        sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_etf_action b ON e.entityid = b.entityid where e.category ILIKE 'Commodities';"

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

# def getETFDetailsById(entity_id):
#     try:
#         sql = "SELECT * FROM tbl_entity  WHERE entityid = %s;"
#         data = (entity_id,)  # tuple, not set
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getting underlying by id query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


# ==================================== Commodities ETF Table End====================================

# ==================================== Equity ETF Table End====================================
def getAllETFEquity():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Equity' AND subcategory ILIKE 'ETF';"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getAllActionTableOfETFEquity():
     try:
        sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_etf_action b ON e.entityid = b.entityid where e.category ILIKE 'Equity';"
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# def getETFDetailsEquityById (entity_id):
#     try:
#         sql = "SELECT * FROM tbl_entity  WHERE entityid = %s;"
#         data = (entity_id,)  # tuple, not set
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getting underlying by id query==========================", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

def getETFEquityDetailActionTable(entity_id):
    try:
        sql = "SELECT * FROM tbl_etf_action WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')



def getETFEquityDetailUnderlyingTable(entity_id):
    try:
        sql = "SELECT u.*, e.scripname FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

# ==================================== Equity Fund ETF Table End====================================


# ==================================== Equity ETF Table End====================================
def getAllFixIncomeETF():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Fixed_Income' AND subcategory ILIKE 'ETF';"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getAllActionTableOfFixIncomeETF():
     try:
        # sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_etf_action b ON e.entityid = b.entityid;"
        sql=" SELECT e.scripname, b.* FROM tbl_entity e join tbl_etf_action b ON e.entityid = b.entityid where e.category ILIKE 'Fixed_Income';"
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getFixIncomeDetailsETFById (entity_id):
    try:
        sql = "SELECT * FROM tbl_entity WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

def getFixIncomeETFDetailActionTable(entity_id):
    try:
        sql = "SELECT * FROM tbl_etf_action  WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')



def getFixIncomeETFDetailUnderlyingTable(entity_id):
    try:
        sql = "SELECT u.*, e.scripname FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

# ==================================== Fix Income Fund ETF Table End====================================


# ==================================== Commodities Direct Table Start==================================

def Insert_CommoditiesDirect(data):
    try:
        sql = "INSERT INTO tbl_commodities_direct (entityid, contract_note_number, trade_date, client_code, client_name, order_number, order_time, trade_number, description, order_type,qty, trade_price, brokerage_per_unit, net_rate_per_unit, gst, stt, security_transaction_tax, exchange_transaction_charges, sebi_turnover_fees,stamp_duty, ipft, net_total, net_amount_receivable, created_at) VALUES (%s, %s, %s::date, %s, %s, %s, %s::time, %s, %s, %s,%s::int, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric,%s::numeric, %s::numeric, %s::numeric, %s::numeric, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')



def getAllDirectEquityCommodities():
     try:
        sql="SELECT * FROM tbl_entity WHERE category ILIKE 'Commodities' AND subcategory ILIKE 'Direct Equity';"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getCommoditiesActionTablebyId (entity_id):
    try:
        sql = "SELECT * FROM  tbl_commodities_direct WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


def getDEDetailCommoditiesEntityById(entity_id):
    try:
        sql = "SELECT * FROM tbl_entity WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')



def getCommoditiesEntity():
     try:
        sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_commodities_direct b ON e.entityid = b.entityid;"
        # sql="SELECT * FROM tbl_entity WHERE category ILIKE 'Commodities' ORDER BY subcategory;"
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')




def getCountOfAllCommodities():
     try:
        #   sql=" SELECT subcategory, MIN(category) AS category,COUNT(*) AS total FROM tbl_entity WHERE (subcategory ILIKE 'ETF' OR subcategory ILIKE 'PMS' OR subcategory ILIKE 'Alternative Investment Funds' OR subcategory ILIKE 'Direct Equity') AND category ILIKE 'Commodities' GROUP BY subcategory ORDER BY subcategory;"
          sql="SELECT subcategory,category,COUNT(*) AS total FROM tbl_entity WHERE category ILIKE 'Commodities' GROUP BY subcategory, category ORDER BY subcategory;"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



def getAllCommoditiesInstrument():
    try:
        action_data = executeSql.FetchAll("SELECT order_type,trade_price_per_unit  FROM tbl_etf_action;")
        print("DEBUG action_data:", action_data)

        aif_data = executeSql.FetchAll("SELECT amc_name, contribution_amount FROM tbl_aif where ")
        print("DEBUG aif_data:", aif_data)

        direct_equity_data = executeSql.FetchAll("SELECT order_type, trade_price FROM tbl_direct_equity")
        print("DEBUG direct_equity_data:", direct_equity_data)

        return {
            "ETC_Action": action_data,
            "aif_data": aif_data,
            "direct_equity_data": direct_equity_data
        }

    except Exception as e:
        print("Error in getAllCommoditiesInstrument==============================", e)
        return {"action_data": [], "aif_data": [], "direct_equity_data": []}



# ==================================== Commodities Ditrect Table End====================================



# ====================================PMS client & AMC actiomn Table Start==================================
def insertClientAction(data):
    try:
        sql = " INSERT INTO tbl_pms_client_action (entityid, pms_order_type, price, cheque, created_at, trade_date) VALUES (%s, %s, %s, %s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


def getAllPmsClientActionTable():
     try:
        #   sql="SELECT * FROM tbl_etf_action;"
        sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_pms_client_action b ON e.entityid = b.entityid;"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def  getPmsClientActionById (entity_id):
    try:
        sql = "SELECT * FROM tbl_pms_client_action WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


def getPmsClientEntity():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Equity' AND subcategory ILIKE 'PMS';"
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def getPmsEquityDetailbyId(entity_id):
     try:
        sql=" SELECT * FROM tbl_entity WHERE entityid = %s;"
        data = (entity_id,)
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


##############-> Amc Action #################3
def insertPmsAmcAction(data):
    try:
        sql = " INSERT INTO tbl_pms_Amc_action (entityid, scrip_name, scrip_code, order_type, quantity, trade_price, net_amount, created_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s)"
        msg = executeSql.ExecuteOne(sql, data)
        return msg
    except Exception as e:
        print("Error in save_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


def getAllPmsAmcActionTable():
     try:
        #   sql="SELECT * FROM tbl_etf_action;"
        sql="SELECT e.scripname, b.* FROM tbl_entity e join tbl_pms_Amc_action b ON e.entityid = b.entityid;"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def  getPmsAmcActionById (entity_id):
    try:
        sql = "SELECT * FROM tbl_pms_Amc_action WHERE entityid = %s;"
        data = (entity_id,)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


def getPmsAmcEntity():
     try:
        sql=" SELECT * FROM tbl_entity WHERE category ILIKE 'Equity' AND subcategory ILIKE 'PMS';"
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')




# ====================================PMS client & AMC action Table END====================================





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

# def getDirectEquityByid(entity_id):
#     try:
#         sql = "SELECT * FROM tbl_entity WHERE entityid = %s;"
#         data = (entity_id,)  # tuple, not set
#         msgs = executeSql.ExecuteAllNew(sql, data)
#         return msgs
#     except Exception as e:
#         print("Error in getting underlying by id query:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')

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


# ======================================Get All BenchMarks======================================
# def getAllEntityBenchMark():
#      try:
#           sql="SELECT benchmark_name FROM tbl_benchmark ORDER BY category;"
#           data=''
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingroleRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getAllEntityBenchMark(category):
    try:
        sql = "SELECT benchmark_name FROM tbl_benchmark WHERE category = %s ORDER BY benchmark_name;"
        data = (category,)   # pass category as parameter
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getAllEntityBenchMark query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1023310')



# ======================================Get All BenchMarks======================================


# ======================================== Get allMfEquityUnderlyingCount Start============================
def getAllEquityUnderlyingCount():
     try:
        sql="WITH EQUITY_SUBCATEGORIES AS ( SELECT DISTINCT SUBCATEGORY FROM TBL_ENTITY WHERE CATEGORY = 'Equity'),MF_NAV AS ( SELECT DISTINCT ON (MF.ISIN)'Mutual Fund' AS SUBCATEGORY, MF.ISIN,  MF.NAV AS NAV_VALUE  FROM TBL_MUTUAL_FUND_NAV MF WHERE EXISTS (SELECT 1 FROM PG_CATALOG.PG_CLASS WHERE RELNAME='tbl_mutual_fund_nav') ORDER BY MF.ISIN, MF.NAV_DATE DESC),AIF_NAV AS ( SELECT DISTINCT ON (AIF.ISIN) 'AIF' AS SUBCATEGORY, AIF.ISIN,  AIF.POST_TAX_NAV AS NAV_VALUE FROM TBL_AIF_NAV AIF WHERE EXISTS (SELECT 1 FROM PG_CATALOG.PG_CLASS WHERE RELNAME='tbl_aif_nav') ORDER BY AIF.ISIN, AIF.NAV_DATE DESC),LATEST_NAV AS ( SELECT * FROM MF_NAV UNION ALL SELECT * FROM AIF_NAV),ENTITY_VALUES AS (  SELECT E.ENTITYID,E.SUBCATEGORY,COALESCE(SUM(A.UNIT * COALESCE(NAV.NAV_VALUE, 1)), 0) AS ENTITY_TOTAL_VALUE FROM TBL_ENTITY E LEFT JOIN TBL_ACTION_TABLE A ON E.ENTITYID = A.ENTITYID LEFT JOIN LATEST_NAV NAV ON A.ISIN = NAV.ISIN AND E.SUBCATEGORY = NAV.SUBCATEGORY WHERE E.CATEGORY = 'Equity'GROUP BY E.ENTITYID, E.SUBCATEGORY),TOTAL_VALUE AS (SELECT SUM(ENTITY_TOTAL_VALUE) AS TOTAL_ALL_ENTITIES FROM ENTITY_VALUES),ENTITY_WEIGHT AS (SELECT EV.ENTITYID,EV.SUBCATEGORY,EV.ENTITY_TOTAL_VALUE,(EV.ENTITY_TOTAL_VALUE / TV.TOTAL_ALL_ENTITIES) AS WEIGHT_FRACTION FROM ENTITY_VALUES EV CROSS JOIN TOTAL_VALUE TV),ENTITY_WEIGHTS AS (SELECT U.ENTITYID,U.TAG,COUNT(*) AS TAG_COUNT,SUM(U.WEIGHTAGE::NUMERIC) AS TAG_WEIGHT FROM TBL_UNDERLYING U JOIN TBL_ENTITY E ON U.ENTITYID = E.ENTITYID WHERE E.CATEGORY = 'Equity' AND U.TAG IS NOT NULL GROUP BY U.ENTITYID, U.TAG),ENTITY_TOTALS AS (SELECT ENTITYID, SUM(TAG_WEIGHT) AS TOTAL_TAG_WEIGHT FROM ENTITY_WEIGHTS GROUP BY ENTITYID),ENTITY_TAG_BREAKDOWN AS (SELECT EW.ENTITYID,EW.TAG,EW.TAG_COUNT,EW.TAG_WEIGHT,ET.TOTAL_TAG_WEIGHT,COALESCE(EMP.WEIGHT_FRACTION, 0) AS ENTITY_WEIGHT_FRACTION,(EW.TAG_WEIGHT / ET.TOTAL_TAG_WEIGHT) * COALESCE(EMP.WEIGHT_FRACTION, 0) AS TAG_FRACTION FROM ENTITY_WEIGHTS EW JOIN ENTITY_TOTALS ET ON EW.ENTITYID = ET.ENTITYID LEFT JOIN ENTITY_WEIGHT EMP ON EW.ENTITYID = EMP.ENTITYID),TAG_TOTALS AS (SELECT TAG,SUM(TAG_COUNT) AS TOTAL_TAG_COUNT,ROUND(SUM(TAG_FRACTION) * 100, 2) AS OVERALL_TAG_PERCENT FROM ENTITY_TAG_BREAKDOWN GROUP BY TAG),GRAND_TOTAL AS (SELECT SUM(TOTAL_TAG_COUNT) AS OVERALL_TAG_COUNT FROM TAG_TOTALS)SELECT TAG,TOTAL_TAG_COUNT,GT.OVERALL_TAG_COUNT,  OVERALL_TAG_PERCENT FROM TAG_TOTALS CROSS JOIN GRAND_TOTAL GT ORDER BY OVERALL_TAG_PERCENT DESC;"

        # "WITH EQUITY_SUBCATEGORIES AS ( SELECT DISTINCT	SUBCATEGORY FROM	TBL_ENTITY WHERE	CATEGORY = 'Equity' ), MF_NAV AS (SELECT DISTINCT	ON (MF.ISIN) 'Mutual Fund' AS SUBCATEGORY,	MF.ISIN,	MF.NAV AS NAV_VALUE FROM	TBL_MUTUAL_FUND_NAV MF WHERE	EXISTS ( SELECT 1 FROM PG_CATALOG.PG_CLASS WHERE RELNAME = 'tbl_mutual_fund_nav' )ORDER BY MF.ISIN, MF.NAV_DATE DESC ), AIF_NAV AS (SELECT DISTINCT ON (AIF.ISIN) 'AIF' AS SUBCATEGORY, AIF.ISIN, AIF.POST_TAX_NAV AS NAV_VALUE FROM TBL_AIF_NAV AIF WHERE EXISTS ( SELECT 1 FROM PG_CATALOG.PG_CLASS WHERE RELNAME = 'tbl_aif_nav' )ORDER BY AIF.ISIN, AIF.NAV_DATE DESC ), LATEST_NAV AS (SELECT * FROM MF_NAV UNION ALL SELECT * FROM AIF_NAV ), ENTITY_VALUES AS (SELECT E.ENTITYID, E.SUBCATEGORY, COALESCE(SUM(A.UNIT * COALESCE(NAV.NAV_VALUE, 1)), 0) AS ENTITY_TOTAL_VALUE FROM TBL_ENTITY E LEFT JOIN TBL_ACTION_TABLE A ON E.ENTITYID = A.ENTITYID LEFT JOIN LATEST_NAV NAV ON A.ISIN = NAV.ISIN AND E.SUBCATEGORY = NAV.SUBCATEGORY WHERE E.CATEGORY = 'Equity'GROUP BY E.ENTITYID, E.SUBCATEGORY ), TOTAL_VALUE AS (SELECT SUM(ENTITY_TOTAL_VALUE) AS TOTAL_ALL_ENTITIES FROM ENTITY_VALUES ), ENTITY_WEIGHT AS (SELECT EV.ENTITYID, EV.SUBCATEGORY, EV.ENTITY_TOTAL_VALUE, (EV.ENTITY_TOTAL_VALUE / TV.TOTAL_ALL_ENTITIES) AS WEIGHT_FRACTION, ROUND( (EV.ENTITY_TOTAL_VALUE / TV.TOTAL_ALL_ENTITIES) * 100, 2 ) AS WEIGHT_PERCENT FROM ENTITY_VALUES EV CROSS JOIN TOTAL_VALUE TV ), ENTITY_WEIGHTS AS (SELECT U.ENTITYID, E.SUBCATEGORY, U.TAG, COUNT(*) AS TAG_COUNT, SUM(U.WEIGHTAGE::NUMERIC) AS TAG_WEIGHT FROM TBL_UNDERLYING U JOIN TBL_ENTITY E ON U.ENTITYID = E.ENTITYID WHERE E.CATEGORY = 'Equity' AND U.TAG IS NOT NULL GROUP BY U.ENTITYID, E.SUBCATEGORY, U.TAG ), ENTITY_TOTALS AS (SELECT ENTITYID, SUM(TAG_WEIGHT) AS TOTAL_TAG_WEIGHT FROM ENTITY_WEIGHTS GROUP BY ENTITYID ), ENTITY_TAG_BREAKDOWN AS (SELECT EW.ENTITYID, EW.SUBCATEGORY, EW.TAG, EW.TAG_COUNT, EW.TAG_WEIGHT, ET.TOTAL_TAG_WEIGHT, COALESCE(EMP.WEIGHT_FRACTION, 0) AS ENTITY_WEIGHT_FRACTION, (EW.TAG_WEIGHT / ET.TOTAL_TAG_WEIGHT) * COALESCE(EMP.WEIGHT_FRACTION, 0) AS TAG_FRACTION FROM ENTITY_WEIGHTS EW JOIN ENTITY_TOTALS ET ON EW.ENTITYID = ET.ENTITYID LEFT JOIN ENTITY_WEIGHT EMP ON EW.ENTITYID = EMP.ENTITYID ), TAG_TOTALS AS (SELECT SUBCATEGORY, TAG, SUM(TAG_COUNT) AS TOTAL_TAG_COUNT, ROUND(SUM(TAG_FRACTION) * 100, 2) AS OVERALL_TAG_PERCENT FROM ENTITY_TAG_BREAKDOWN GROUP BY SUBCATEGORY, TAG ), TAG_TOTALS_GLOBAL AS (SELECT TAG, SUM(TOTAL_TAG_COUNT) AS TOTAL_TAG_COUNT_ALL_ENTITIES FROM TAG_TOTALS GROUP BY TAG ), TAG_TOTALS_WITH_ROW AS (SELECT TT.SUBCATEGORY, TT.TAG, TT.TOTAL_TAG_COUNT, TT.OVERALL_TAG_PERCENT, TGG.TOTAL_TAG_COUNT_ALL_ENTITIES, ROW_NUMBER() OVER ( PARTITION BY TT.TAG ORDER BY TT.SUBCATEGORY DESC ) AS RN FROM TAG_TOTALS TT JOIN TAG_TOTALS_GLOBAL TGG ON TT.TAG = TGG.TAG ), GRAND_TOTAL AS (SELECT SUM(TOTAL_TAG_COUNT) AS OVERALL_TAG_COUNT FROM TAG_TOTALS ), TOTAL_EQUITY_UNDERLYINGS AS (SELECT COUNT(*) AS TOTAL_EQUITY_UNDERLYINGS FROM TBL_UNDERLYING U JOIN TBL_ENTITY E ON U.ENTITYID = E.ENTITYID WHERE E.CATEGORY = 'Equity' ) SELECT SUBCATEGORY, TAG, TOTAL_TAG_COUNT, CASE WHEN RN = 1 THEN TOTAL_TAG_COUNT_ALL_ENTITIES ELSE NULL END AS TOTAL_TAG_COUNT_ALL_ENTITIES, GT.OVERALL_TAG_COUNT, OVERALL_TAG_PERCENT, TEU.TOTAL_EQUITY_UNDERLYINGS FROM TAG_TOTALS_WITH_ROW CROSS JOIN GRAND_TOTAL GT CROSS JOIN TOTAL_EQUITY_UNDERLYINGS TEU ORDER BY TAG, SUBCATEGORY;"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')



def GetallMfEquityUnderlyingCount():
     try:
        # sql="WITH latest_nav AS (SELECT DISTINCT ON (isin) isin, nav FROM tbl_mutual_fund_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT a.entityid, SUM(a.unit * nav.nav) AS entity_total_value FROM tbl_action_table a JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY a.entityid),total_value AS ( SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values), entity_mf_percent AS ( SELECT ev.entityid,ev.entity_total_value, (ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv), entity_weights AS ( SELECT u.entityid, u.tag, COUNT(*) AS tag_count, SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.tag IS NOT NULL GROUP BY u.entityid, u.tag), entity_totals AS ( SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid ), entity_tag_breakdown AS ( SELECT ew.entityid, ew.tag, ew.tag_count, ew.tag_weight, et.total_tag_weight, ev.entity_total_value,emp.mf_weight_percent, (ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction, ROUND((ew.tag_weight / et.total_tag_weight) * emp.mf_weight_percent, 2) AS total_tag_percent FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_values ev ON ew.entityid = ev.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid), tag_totals AS ( SELECT tag, ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag) SELECT etb.entityid, etb.tag, etb.tag_count, etb.tag_weight, etb.total_tag_weight, etb.entity_total_value, etb.mf_weight_percent, etb.total_tag_percent, CASE  WHEN ROW_NUMBER() OVER (PARTITION BY etb.tag ORDER BY etb.entityid) = 1 THEN tt.overall_tag_percent END AS overall_tag_percent FROM entity_tag_breakdown etb JOIN tag_totals tt ON etb.tag = tt.tag ORDER BY etb.tag, etb.entityid;"

        # sql="WITH latest_nav AS (SELECT DISTINCT ON (isin) isin, nav FROM tbl_mutual_fund_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT a.entityid, SUM(a.unit * nav.nav) AS entity_total_value FROM tbl_action_table a JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY a.entityid), total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values), entity_mf_percent AS (SELECT ev.entityid, ev.entity_total_value, (ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction, ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv),entity_weights AS ( SELECT u.entityid, u.tag, SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.tag IS NOT NULL GROUP BY u.entityid, u.tag ), entity_totals AS ( SELECT entityid,SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid ),entity_tag_breakdown AS ( SELECT ew.entityid, ew.tag, ew.tag_weight, et.total_tag_weight, ev.entity_total_value, emp.mf_weight_percent,(ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction, ROUND((ew.tag_weight / et.total_tag_weight) * emp.mf_weight_percent, 2) AS total_tag_percent FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_values ev ON ew.entityid = ev.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT tag, ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag)SELECT tag, overall_tag_percent FROM tag_totals ORDER BY overall_tag_percent DESC;"

        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin) isin, nav FROM tbl_mutual_fund_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT a.entityid,SUM(a.unit * nav.nav) AS entity_total_value FROM tbl_action_table a JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY a.entityid),total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values ),entity_mf_percent AS (SELECT ev.entityid, ev.entity_total_value,(ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction, ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv),entity_weights AS (SELECT u.entityid,u.tag,COUNT(*) AS tag_count,SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.tag IS NOT NULL AND e.category = 'Equity'AND e.subcategory = 'Mutual Fund' GROUP BY u.entityid, u.tag),entity_totals AS (SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid ),entity_tag_breakdown AS (SELECT ew.entityid,ew.tag,ew.tag_count,ew.tag_weight,et.total_tag_weight,emp.mf_weight_percent,(ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT tag,SUM(tag_count) AS total_tag_count,ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag),grand_total AS (SELECT SUM(total_tag_count) AS overall_tag_count FROM tag_totals )SELECT tt.tag, tt.total_tag_count, gt.overall_tag_count,tt.overall_tag_percent FROM tag_totals tt CROSS JOIN grand_total gt ORDER BY tt.overall_tag_percent DESC;"

        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

def GetallMfSectorUnderlyingCount():
     try:
        #   sql="WITH counts AS (SELECT u.sector,COUNT(*) AS sector_count,(SELECT COUNT(*) FROM tbl_underlying u2 JOIN tbl_entity e2 ON u2.entityid = e2.entityid WHERE e2.category = 'Equity' AND e2.subcategory = 'Mutual Fund') AS total_mf_count FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY u.sector) SELECT sector ,sector_count,total_mf_count,(sector_count * 100.0 / total_mf_count)::numeric(5,2) AS sector_percent FROM counts ORDER BY sector_percent DESC;"
        #   sql="WITH counts AS (SELECT u.sector,COUNT(*) AS sector_count,SUM(u.weightage::numeric) AS sector_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.sector IS NOT NULL GROUP BY u.sector),total AS (SELECT COUNT(*) AS total_sector_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.sector IS NOT NULL) SELECT c.sector,c.sector_count,t.total_sector_count,c.sector_weight,t.total_weight,(c.sector_weight * 100.0 / t.total_weight)::numeric(7,2) AS sector_percent FROM counts c CROSS JOIN total t ORDER BY sector_percent DESC;"
        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin) isin, nav FROM tbl_mutual_fund_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT a.entityid, SUM(a.unit * nav.nav) AS entity_total_value FROM tbl_action_table a JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY a.entityid),total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values),entity_mf_percent AS (SELECT ev.entityid,ev.entity_total_value,(ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction, ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv),entity_weights AS (SELECT u.entityid,u.sector,COUNT(*) AS tag_count,SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.sector IS NOT NULL AND e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY u.entityid, u.sector ), entity_totals AS ( SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid), entity_tag_breakdown AS (SELECT ew.entityid, ew.sector, ew.tag_count,ew.tag_weight,et.total_tag_weight,emp.mf_weight_percent,(ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT sector AS sector,SUM(tag_count) AS total_tag_count,ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY sector),grand_total AS (SELECT SUM(total_tag_count) AS overall_tag_count FROM tag_totals)SELECT tt.sector,tt.total_tag_count,gt.overall_tag_count,tt.overall_tag_percent FROM tag_totals tt CROSS JOIN grand_total gt ORDER BY tt.overall_tag_percent DESC;"
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# def getallMFDtailEquitySectorCount():
#      try:
#           sql="WITH counts AS (SELECT u.sector,COUNT(*) AS sector_count,(SELECT COUNT(*) FROM tbl_underlying u2 JOIN tbl_entity e2 ON u2.entityid = e2.entityid WHERE e2.category = 'Equity' AND e2.subcategory = 'Mutual Fund') AS total_mf_count FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY u.sector)SELECT u.entityid,u.sector,c.sector_count,c.total_mf_count,(c.sector_count * 100.0 / c.total_mf_count)::numeric(5,2) AS sector_percent FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid JOIN counts c ON u.sector = c.sector WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' ORDER BY sector_percent DESC;"
#           data=''
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingroleRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')


def getallMFDetailsEquitySectorCount(entity_id):
    try:
        # sql = "WITH counts AS (SELECT u.sector,COUNT(*) AS sector_count,(SELECT COUNT(*) FROM tbl_underlying u2 JOIN tbl_entity e2 ON u2.entityid = e2.entityid WHERE e2.category = 'Equity' AND e2.subcategory = 'Mutual Fund') AS total_mf_count FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' GROUP BY u.sector)SELECT DISTINCT u.entityid,u.sector,c.sector_count,c.total_mf_count,(c.sector_count * 100.0 / c.total_mf_count)::numeric(5,2) AS sector_percent FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid JOIN counts c ON u.sector = c.sector WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund'AND u.entityid = %s;"

        # sql="WITH all_tags AS (SELECT DISTINCT sector FROM tbl_underlying WHERE sector IS NOT NULL),entity_weights AS (SELECT u.sector, SUM(u.weightage::numeric) AS sector_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.entityid = %s AND u.sector IS NOT NULL GROUP BY u.sector),total AS (SELECT SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.entityid = %s AND u.sector IS NOT NULL)SELECT t.sector,COALESCE(ew.sector_weight, 0) AS sector_weight,total.total_weight,COALESCE((ew.sector_weight * 100.0 / total.total_weight)::numeric(7,2),0.00) AS sector_percent FROM all_tags t LEFT JOIN entity_weights ew ON t.sector = ew.sector CROSS JOIN total ORDER BY t.sector;"

        sql="WITH entity_weights AS (SELECT u.sector,COUNT(*) AS sector_count,SUM(u.weightage::numeric) AS sector_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.entityid =%s AND u.sector IS NOT NULL GROUP BY u.sector),total AS (SELECT COUNT(*) AS total_sector_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.entityid =%s AND u.sector IS NOT NULL)  SELECT ew.sector,ew.sector_count,total.total_sector_count,ew.sector_weight,total.total_weight,(ew.sector_weight * 100.0 / total.total_weight)::numeric(7,2) AS sector_percent FROM entity_weights ew CROSS JOIN total ORDER BY sector_percent DESC;"

        data = (entity_id, entity_id)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


def getallMFDetailsEquityMCAPCount(entity_id):
    try:
        # sql="WITH all_tags AS ( SELECT DISTINCT tag FROM tbl_underlying WHERE tag IS NOT NULL),entity_counts AS (SELECT u.tag, COUNT(*) AS tag_count FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund'AND u.entityid = %s AND u.tag IS NOT NULL GROUP BY u.tag), total AS (SELECT COUNT(*) AS total_mf_count FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.tag IS NOT NULL) SELECT t.tag, COALESCE(ec.tag_count, 0) AS tag_count,total.total_mf_count,COALESCE((ec.tag_count * 100.0 / total.total_mf_count)::numeric(5,2), 0.00) AS tag_percent FROM all_tags t LEFT JOIN entity_counts ec ON t.tag = ec.tag CROSS JOIN total ORDER BY t.tag;"
        sql="WITH all_tags AS (SELECT DISTINCT tag FROM tbl_underlying WHERE tag IS NOT NULL),entity_weights AS (SELECT u.tag, COUNT(*) AS tag_count,SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.entityid =%s AND u.tag IS NOT NULL GROUP BY u.tag),total AS (SELECT COUNT(*) AS total_tag_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Mutual Fund' AND u.entityid = %s AND u.tag IS NOT NULL) SELECT t.tag,COALESCE(ew.tag_count, 0) AS tag_count,total.total_tag_count,COALESCE(ew.tag_weight, 0) AS tag_weight,total.total_weight,COALESCE((ew.tag_weight * 100.0 / total.total_weight)::numeric(7,2), 0.00) AS tag_percent FROM all_tags t LEFT JOIN entity_weights ew ON t.tag = ew.tag CROSS JOIN total ORDER BY tag_percent DESC;"

        data = (entity_id, entity_id)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')




def getAIFDetailsEquityMCAPCount(entity_id):
    try:
        sql="WITH all_tags AS (SELECT DISTINCT tag FROM tbl_underlying WHERE tag IS NOT NULL),entity_weights AS (SELECT u.tag, COUNT(*) AS tag_count,SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s AND u.tag IS NOT NULL GROUP BY u.tag),total AS (SELECT COUNT(*) AS total_tag_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s AND u.tag IS NOT NULL) SELECT t.tag,COALESCE(ew.tag_count, 0) AS tag_count,total.total_tag_count,COALESCE(ew.tag_weight, 0) AS tag_weight,total.total_weight,COALESCE((ew.tag_weight * 100.0 / total.total_weight)::numeric(7,2), 0.00) AS tag_percent FROM all_tags t LEFT JOIN entity_weights ew ON t.tag = ew.tag CROSS JOIN total ORDER BY tag_weight DESC;"

        data = (entity_id, entity_id)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')


def getAIFDetailsEquitySectorCount(entity_id):
    try:
        sql="WITH entity_weights AS (SELECT u.sector,COUNT(*) AS sector_count,SUM(u.weightage::numeric) AS sector_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s AND u.sector IS NOT NULL GROUP BY u.sector),total AS (SELECT COUNT(*) AS total_sector_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Equity' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s AND u.sector IS NOT NULL)  SELECT ew.sector,ew.sector_count,total.total_sector_count,ew.sector_weight,total.total_weight,(ew.sector_weight * 100.0 / total.total_weight)::numeric(7,2) AS sector_percent FROM entity_weights ew CROSS JOIN total ORDER BY sector_percent DESC;"

        data = (entity_id, entity_id)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')
    


def GetallAifEquityUnderlyingCount():
    try:
        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin)	isin,	post_tax_nav AS nav FROM tbl_aif_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT	a.entityid,	SUM(a.num_units * COALESCE(nav.nav, 1)) AS entity_total_value FROM tbl_aif a LEFT JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity'  AND e.subcategory = 'Alternative Investment Funds' GROUP BY a.entityid),total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values),entity_mf_percent AS (SELECT	ev.entityid,	ev.entity_total_value,	(ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,	ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv),entity_weights AS (SELECT	u.entityid,	u.tag,	COUNT(*) AS tag_count,	SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.tag IS NOT NULL  AND e.category = 'Equity'  AND e.subcategory = 'Alternative Investment Funds' GROUP BY u.entityid, u.tag),entity_totals AS (SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid),entity_tag_breakdown AS (SELECT	ew.entityid,	ew.tag,	ew.tag_count,	ew.tag_weight,	et.total_tag_weight,	emp.mf_weight_percent,	(ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT	tag,	SUM(tag_count) AS total_tag_count,	ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag),grand_total AS (SELECT SUM(total_tag_count) AS overall_tag_count FROM tag_totals)SELECT tt.tag,tt.total_tag_count,gt.overall_tag_count,tt.overall_tag_percent FROM tag_totals tt CROSS JOIN grand_total gt ORDER BY tt.overall_tag_percent DESC;"      
        data=''       
        msgs=executeSql.ExecuteAllNew(sql,data)        
        return msgs    
    except Exception as e:
        print("Error in getingroleRecord query==========================",e)
        return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')    


def GetallAIFSectorUnderlyingCount():
     try:
        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin)    isin,  post_tax_nav AS nav FROM tbl_aif_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT    a.entityid,    SUM(a.num_units * COALESCE(nav.nav, 1)) AS entity_total_value FROM tbl_aif a LEFT JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Equity'  AND e.subcategory = 'Alternative Investment Funds' GROUP BY a.entityid),total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values),entity_mf_percent AS (SELECT    ev.entityid,    ev.entity_total_value,    (ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,    ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv), entity_weights AS (SELECT    u.entityid,    u.sector,    COUNT(*) AS tag_count,    SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.sector IS NOT NULL  AND e.category = 'Equity'  AND e.subcategory = 'Alternative Investment Funds'GROUP BY u.entityid, u.sector),entity_totals AS (SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid),entity_tag_breakdown AS (SELECT    ew.entityid,    ew.sector,    ew.tag_count,    ew.tag_weight,    et.total_tag_weight,    emp.mf_weight_percent,    (ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT    sector,    SUM(tag_count) AS total_tag_count,    ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY sector),grand_total AS (SELECT SUM(total_tag_count) AS overall_tag_count FROM tag_totals)SELECT tt.sector,tt.total_tag_count,gt.overall_tag_count,tt.overall_tag_percent FROM tag_totals tt CROSS JOIN grand_total gt ORDER BY tt.overall_tag_percent DESC;"    
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')   



def getAIFDetailsFixedIncomeMCAPCount(entity_id):
    try:
        sql="WITH all_tags AS (SELECT DISTINCT tag FROM tbl_underlying WHERE tag IS NOT NULL),entity_weights AS (SELECT u.tag, COUNT(*) AS tag_count,SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Fixed_Income' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s AND u.tag IS NOT NULL GROUP BY u.tag),total AS (SELECT COUNT(*) AS total_tag_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Fixed_Income' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid =  %s AND u.tag IS NOT NULL) SELECT t.tag,COALESCE(ew.tag_count, 0) AS tag_count,total.total_tag_count,COALESCE(ew.tag_weight, 0) AS tag_weight,total.total_weight,COALESCE((ew.tag_weight * 100.0 / total.total_weight)::numeric(7,2), 0.00) AS tag_percent FROM all_tags t LEFT JOIN entity_weights ew ON t.tag = ew.tag CROSS JOIN total ORDER BY tag_weight DESC;"

        data = (entity_id, entity_id)  # tuple, not set
        msgs = executeSql.ExecuteAllNew(sql, data)
        return msgs
    except Exception as e:
        print("Error in getting underlying by id query:", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1022310')
    
def GetallFixedIncomeUnderlyingCount():
    try:
        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin)	isin,	post_tax_nav AS nav FROM tbl_aif_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT	a.entityid,	SUM(a.num_units * COALESCE(nav.nav, 1)) AS entity_total_value FROM tbl_aif a LEFT JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Fixed_Income'  AND e.subcategory = 'Alternative Investment Funds' GROUP BY a.entityid),total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values),entity_mf_percent AS (SELECT	ev.entityid,	ev.entity_total_value,	(ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,	ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv),entity_weights AS (SELECT	u.entityid,	u.tag,	COUNT(*) AS tag_count,	SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.tag IS NOT NULL  AND e.category = 'Fixed_Income'  AND e.subcategory = 'Alternative Investment Funds' GROUP BY u.entityid, u.tag),entity_totals AS (SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid),entity_tag_breakdown AS (SELECT	ew.entityid,	ew.tag,	ew.tag_count,	ew.tag_weight,	et.total_tag_weight,	emp.mf_weight_percent,	(ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT	tag,	SUM(tag_count) AS total_tag_count,	ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY tag),grand_total AS (SELECT SUM(total_tag_count) AS overall_tag_count FROM tag_totals)SELECT tt.tag,tt.total_tag_count,gt.overall_tag_count,tt.overall_tag_percent FROM tag_totals tt CROSS JOIN grand_total gt ORDER BY tt.overall_tag_percent DESC;"      
        data=''       
        msgs=executeSql.ExecuteAllNew(sql,data)        
        return msgs    
    except Exception as e:
        print("Error in getingroleRecord query==========================",e)
        return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')   

def GetallAIFFixedIncomeSectorCount():
     try:
        sql="WITH latest_nav AS (SELECT DISTINCT ON (isin)    isin,  post_tax_nav AS nav FROM tbl_aif_nav ORDER BY isin, nav_date DESC),entity_values AS (SELECT    a.entityid,    SUM(a.num_units * COALESCE(nav.nav, 1)) AS entity_total_value FROM tbl_aif a LEFT JOIN latest_nav nav ON a.isin = nav.isin JOIN tbl_entity e ON a.entityid = e.entityid WHERE e.category = 'Fixed_Income'  AND e.subcategory = 'Alternative Investment Funds' GROUP BY a.entityid),total_value AS (SELECT SUM(entity_total_value) AS total_all_entities FROM entity_values),entity_mf_percent AS (SELECT    ev.entityid,    ev.entity_total_value,    (ev.entity_total_value / tv.total_all_entities) AS mf_weight_fraction,    ROUND((ev.entity_total_value / tv.total_all_entities) * 100, 2) AS mf_weight_percent FROM entity_values ev CROSS JOIN total_value tv), entity_weights AS (SELECT    u.entityid,    u.sector,    COUNT(*) AS tag_count,    SUM(u.weightage::numeric) AS tag_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE u.sector IS NOT NULL  AND e.category = 'Fixed_Income'  AND e.subcategory = 'Alternative Investment Funds'GROUP BY u.entityid, u.sector),entity_totals AS (SELECT entityid, SUM(tag_weight) AS total_tag_weight FROM entity_weights GROUP BY entityid),entity_tag_breakdown AS (SELECT    ew.entityid,    ew.sector,    ew.tag_count,    ew.tag_weight,    et.total_tag_weight,    emp.mf_weight_percent,    (ew.tag_weight / et.total_tag_weight) * emp.mf_weight_fraction AS tag_fraction FROM entity_weights ew JOIN entity_totals et ON ew.entityid = et.entityid JOIN entity_mf_percent emp ON ew.entityid = emp.entityid),tag_totals AS (SELECT    sector,    SUM(tag_count) AS total_tag_count,    ROUND(SUM(tag_fraction) * 100, 2) AS overall_tag_percent FROM entity_tag_breakdown GROUP BY sector),grand_total AS (SELECT SUM(total_tag_count) AS overall_tag_count FROM tag_totals)SELECT tt.sector,tt.total_tag_count,gt.overall_tag_count,tt.overall_tag_percent FROM tag_totals tt CROSS JOIN grand_total gt ORDER BY tt.overall_tag_percent DESC;"    
        data=''
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')        



def getAIFDetailsFixedIncomeSectorCount(entity_id):
     try:
        sql="WITH entity_weights AS (SELECT u.sector,COUNT(*) AS sector_count,SUM(u.weightage::numeric) AS sector_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Fixed_Income' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s AND u.sector IS NOT NULL GROUP BY u.sector),total AS (SELECT COUNT(*) AS total_sector_count,SUM(u.weightage::numeric) AS total_weight FROM tbl_underlying u JOIN tbl_entity e ON u.entityid = e.entityid WHERE e.category = 'Fixed_Income' AND e.subcategory = 'Alternative Investment Funds' AND u.entityid = %s  AND u.sector IS NOT NULL)  SELECT ew.sector,ew.sector_count,total.total_sector_count,ew.sector_weight,total.total_weight,(ew.sector_weight * 100.0 / total.total_weight)::numeric(7,2) AS sector_percent FROM entity_weights ew CROSS JOIN total ORDER BY sector_percent DESC;"     
        data=(entity_id,entity_id)
        msgs=executeSql.ExecuteAllNew(sql,data)
        return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')                


# ======================================== Get allMfEquityUnderlyingCount END ============================



# ======================================Get All Home instrument_counts of Equity======================================
def getAllHomeData():
     try:
          sql="WITH instrument_counts AS (SELECT 'Equity' AS instrument, (SELECT COUNT(*) FROM tbl_action_table) + (SELECT COUNT(*) FROM tbl_aif) + (SELECT COUNT(*) FROM tbl_direct_equity) AS instrument_total_count UNION ALL SELECT 'Fixed Income', 0 UNION ALL SELECT 'Commodities', 0)SELECT * FROM instrument_counts;"
          data=''
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in getingroleRecord query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')
# ======================================Get All Home instrument_counts of Equity======================================



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
    sql = """
        SELECT order_date::date, TRIM(LOWER(order_type)) AS order_type,
               purchase_amount, redeem_amount, isin, unit
        FROM tbl_action_table
        WHERE TRIM(entityid) ILIKE %s
        ORDER BY order_date;
    """
    return executeSql.ExecuteAllWithHeaders(sql, (entityid,))


def get_latest_nav(isin):
    sql = """
        SELECT nav
        FROM tbl_mutual_fund_nav
        WHERE isin = %s
        ORDER BY nav_date DESC
        LIMIT 1;
    """
    row = executeSql.ExecuteAllWithHeaders(sql, (isin,))
    if row:
        return float(row[0].get("nav") or 0)
    return 0.0


# old
# def get_cashflows_action(entityid):
#     sql = "SELECT order_date::date, TRIM(LOWER(order_type)) AS order_type,purchase_amount, redeem_amount FROM tbl_action_table WHERE TRIM(entityid) ILIKE %s ORDER BY order_date;"
#     rows = executeSql.ExecuteAllWithHeaders(sql, (entityid,))

#     cashflows, dates = [], []
#     for r in rows:
#         order_type = (r.get("order_type") or "").strip().lower()

#         if order_type in ("purchase", "buy"):
#             amt = float(r.get("purchase_amount") or 0)
#             if amt > 0:
#                 cashflows.append(-amt)
#                 dates.append(r["order_date"])

#         elif order_type in ("sell", "redeem", "redemption"):
#             # first try redeem_amount, but if it's 0 and purchase_amount > 0, use that
#             amt = float(r.get("redeem_amount") or 0)
#             if amt <= 0:
#                 amt = float(r.get("purchase_amount") or 0)
#             if amt > 0:
#                 cashflows.append(amt)
#                 dates.append(r["order_date"])

#     return cashflows, dates
# old
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




def get_cashflows_All_action():
    sql = "SELECT order_date::date, TRIM(LOWER(order_type)) AS order_type,purchase_amount, redeem_amount FROM tbl_action_table ORDER BY order_date;"
    rows = executeSql.ExecuteAllWithHeaders(sql)

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



def getDirectEquityCommodityIRR(entityid):
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

# ======================================calculate Xirr (IRR)======================================





# ============================= Auto PDF Read and Insert Into DB  Start queries=========================
# Insert entity, letting Postgres trigger generate entityid

# def order_exists_for_entity(order_number, entityid):
#     try:
#         sql = """
#             SELECT 1 FROM tbl_action_table
#             WHERE order_number = %s AND entityid = %s
#             LIMIT 1
#         """
#         result = executeSql.ExecuteAllNew(sql, (order_number, entityid))
#         return len(result) > 0
#     except Exception as e:
#         print("Error in order_exists_for_entity:", e)
#         return False

# def create_entity(scripname, scripcode, benchmark, category, subcategory, nickname, isin, created_at):
#     """
#     Always inserts a new entity and returns its new entityid.
#     Trigger on tbl_entity will auto-generate entityid like ENT-0001.
#     """
#     try:
#         sql_insert = """
#             INSERT INTO tbl_entity
#             (scripname, scripcode, benchmark, category, subcategory, nickname, isin, created_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         """
#         executeSql.ExecuteOne(sql_insert, (scripname, scripcode, benchmark, category, subcategory, nickname, isin, created_at))

#         # Get the newly generated entityid
#         sql_last = "SELECT entityid FROM tbl_entity ORDER BY id DESC LIMIT 1"
#         last = executeSql.ExecuteAllNew(sql_last, ())
#         if last and len(last) > 0:
#             return last[0]["entityid"]

#         return None

#     except Exception as e:
#         print("Error in create_entity:", e)
#         return None

# # ------------------------------
# # Insert multiple actions per entity
# # ------------------------------
# def auto_action_table(action_tuple):
#     """
#     Insert a single mutual fund action record.
#     action_tuple must match the table columns in order.
#     """
#     try:
#         sql = """
#             INSERT INTO tbl_action_table
#             (scrip_code, mode, order_type, scrip_name, isin, order_number, folio_number, nav, stt,
#              unit, redeem_amount, purchase_amount, cgst, sgst, igst, ugst, stamp_duty, cess_value,
#              net_amount, created_at, entityid, purchase_value, order_date, sett_no)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """
#         executeSql.ExecuteOne(sql, action_tuple)
#     except Exception as e:
#         print("Error in auto_action_table:", e)


# # ------------------------------
# # Insert PDF file for an entity
# # ------------------------------
# def insert_pdf_file(entityid, pdf_name, pdf_file, uploaded_at):
#     """
#     Inserts a PDF file record for a specific entityid.
#     """
#     try:
#         sql = """
#             INSERT INTO tbl_action_pdf
#             (entityid, pdf_name, pdf_file, uploaded_at)
#             VALUES (%s, %s, %s, %s)
#         """
#         executeSql.ExecuteOne(sql, (entityid, pdf_name, pdf_file, uploaded_at))
#     except Exception as e:
#         print("Error in insert_pdf_file:", e)



# /////////////////////

def order_exists(order_number):
    """
    Check if an order_number already exists in tbl_action_table (any entity).
    """
    try:
        sql = """
            SELECT 1 FROM tbl_action_table
            WHERE order_number = %s
            LIMIT 1
        """
        result = executeSql.ExecuteAllNew(sql, (order_number,))
        return len(result) > 0
    except Exception as e:
        print("Error in order_exists:", e)
        return False


def create_entity(scripname, scripcode, benchmark, category, subcategory, nickname, isin, created_at):
    """
    Always inserts a new entity and returns its new entityid.
    Trigger on tbl_entity will auto-generate entityid like ENT-0001.
    """
    try:
        sql_insert = """
            INSERT INTO tbl_entity
            (scripname, scripcode, benchmark, category, subcategory, nickname, isin, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        executeSql.ExecuteOne(sql_insert, (scripname, scripcode, benchmark, category, subcategory, nickname, isin, created_at))

        # Get the newly generated entityid
        sql_last = "SELECT entityid FROM tbl_entity ORDER BY id DESC LIMIT 1"
        last = executeSql.ExecuteAllNew(sql_last, ())
        if last and len(last) > 0:
            return last[0]["entityid"]

        return None

    except Exception as e:
        print("Error in create_entity:", e)
        return None


def auto_action_table(action_tuple):
    """
    Insert a single mutual fund action record.
    Returns True if inserted, False if duplicate.
    """
    try:
        sql = """
            INSERT INTO tbl_action_table
            (scrip_code, mode, order_type, scrip_name, isin, order_number, folio_number, nav, stt,
             unit, redeem_amount, purchase_amount, cgst, sgst, igst, ugst, stamp_duty, cess_value,
             net_amount, created_at, entityid, purchase_value, order_date, sett_no)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        executeSql.ExecuteOne(sql, action_tuple)
        return True  # ✅ Insert success

    except Exception as e:
        # Detect duplicate from DB-level constraint
        if "duplicate key value violates unique constraint" in str(e):
            return False  # ⚠️ Skipped due to duplicate
        print("Error in auto_action_table:", e)
        return False



def insert_pdf_file(entityid, pdf_name, pdf_file, uploaded_at):
    """
    Inserts a PDF file record for a specific entityid.
    """
    try:
        sql = """
            INSERT INTO tbl_action_pdf
            (entityid, pdf_name, pdf_file, uploaded_at)
            VALUES (%s, %s, %s, %s)
        """
        executeSql.ExecuteOne(sql, (entityid, pdf_name, pdf_file, uploaded_at))
    except Exception as e:
        print("Error in insert_pdf_file:", e)

# =================== AIF ===================
# def auto_InsertAifData(data):
#     try:
#         sql = """INSERT INTO tbl_aif
#                  (entityid, trans_date, trans_type, contribution_amount, setup_expense, stamp_duty,
#                   amount_invested, post_tax_nav, num_units, balance_units, strategy_name, amc_name, created_at)
#                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#         log_sql(sql, data)
#         msg = executeSql.ExecuteOne(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in auto_InsertAifData:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

# # =================== ETF ===================
# def auto_InsertEtfData(data):
#     try:
#         sql = """INSERT INTO tbl_etf_action
#                  (entityid, order_number, order_time, trade_number, trade_time, security_description, order_type,
#                   quantity, gross_rate, trade_price_per_unit, brokerage_per_unit, net_rate_per_unit, closing_rate,
#                   gst, stt, net_total_before_levies, remarks, created_at, trade_date)
#                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#         log_sql(sql, data)
#         msg = executeSql.ExecuteOne(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in auto_InsertEtfData:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

# # =================== Commodities ===================
# def auto_insertcommoditiesDirect(data):
#     try:
#         sql = """INSERT INTO tbl_commodities_direct
#                  (entityid, contract_note_number, trade_date, client_code, client_name, order_number, order_time,
#                   trade_number, description, order_type, qty, trade_price, brokerage_per_unit, net_rate_per_unit,
#                   gst, stt, security_transaction_tax, exchange_transaction_charges, sebi_turnover_fees, stamp_duty,
#                   ipft, net_total, net_amount_receivable, created_at)
#                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#         log_sql(sql, data)
#         msg = executeSql.ExecuteOne(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in auto_insertcommoditiesDirect:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')

# # =================== Direct Equity ===================
# def auto_insert_directdata(data):
#     try:
#         sql = """INSERT INTO tbl_direct_equity
#                  (entityid, contract_note_number, trade_date, client_code, client_name, order_number, order_time,
#                   trade_number, description, order_type, qty, trade_price, brokerage_per_unit, net_rate_per_unit,
#                   gst, stt, security_transaction_tax, exchange_transaction_charges, sebi_turnover_fees, stamp_duty,
#                   ipft, net_total, net_amount_receivable, created_at)
#                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#         log_sql(sql, data)
#         msg = executeSql.ExecuteOne(sql, data)
#         return msg
#     except Exception as e:
#         print("Error in auto_insert_directdata:", e)
#         return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020310')


# ============================= Auto PDF Read and Insert Into DB  END queries=========================

# =======================# new compare weight API Start========================
def compare_entity_weights(entity1, entity2):
    """
    Compare two entities (mutual funds or portfolios) by holdings and weights.
    - Returns all holdings of each entity
    - Overlap details, counts, and percentages
    - Uses scrip_name as keys
    - Handles missing data gracefully
    """
    try:
        # Fetch holdings along with scrip_name
        sql = """
            SELECT u.entityid, e.scripname, u.company_name u.isin_code, u.weightage::numeric AS weight
            FROM tbl_underlying u
            JOIN tbl_entity e ON u.entityid = e.entityid
            WHERE u.entityid IN (%s, %s)
        """
        rows = executeSql.ExecuteAllWithHeaders(sql, (entity1, entity2))
        df = pd.DataFrame(rows)

        # Separate holdings for each entity
        df1 = df[df['entityid'] == entity1].copy() if not df.empty else pd.DataFrame(columns=['isin_code', 'weight', 'scripname'])
        df2 = df[df['entityid'] == entity2].copy() if not df.empty else pd.DataFrame(columns=['isin_code', 'weight', 'scripname'])

        # Extract scrip_name (fallback to entityid if missing)
        entity1_name = df1['scripname'].iloc[0] if not df1.empty else entity1
        entity2_name = df2['scripname'].iloc[0] if not df2.empty else entity2

        # Prepare holdings data
        entity1_holdings = df1[['isin_code','weight']].rename(columns={'isin_code':'isin'}).to_dict(orient='records')
        entity2_holdings = df2[['isin_code','weight']].rename(columns={'isin_code':'isin'}).to_dict(orient='records')

        # Merge to find overlaps
        if not df1.empty and not df2.empty:
            merged = pd.merge(df1, df2, on='isin_code', suffixes=('_mf1','_mf2'))
            merged['overlap_weight'] = merged[['weight_mf1','weight_mf2']].min(axis=1)
        else:
            merged = pd.DataFrame(columns=['isin_code','weight_mf1','weight_mf2','overlap_weight'])

        # Calculate totals
        total_overlap = merged['overlap_weight'].sum() if not merged.empty else 0
        overlap_count = len(merged)
        total_mf1_weight = df1['weight'].sum() if not df1.empty else 0
        total_mf2_weight = df2['weight'].sum() if not df2.empty else 0

        # Prepare final response using scrip_name as keys
        result = {
            entity1_name: {
                "id": entity1,
                "total_holdings": len(df1),
                "total_weight": round(float(total_mf1_weight), 2),
                "holdings": entity1_holdings
            },
            entity2_name: {
                "id": entity2,
                "total_holdings": len(df2),
                "total_weight": round(float(total_mf2_weight), 2),
                "holdings": entity2_holdings
            },
            "overlap": {
                "count": overlap_count,
                "total_overlap_weight": round(float(total_overlap),2),
                "overlap_percentage_of_mf1": round((total_overlap / total_mf1_weight) * 100,2) if total_mf1_weight else 0,
                "overlap_percentage_of_mf2": round((total_overlap / total_mf2_weight) * 100,2) if total_mf2_weight else 0,
                "details": merged[['isin_code','weight_mf1','weight_mf2','overlap_weight']]
                            .rename(columns={'isin_code':'isin'}).to_dict(orient='records') if not merged.empty else []
            }
        }

        return result

    except Exception as e:
        print("Error in compare_entity_weights:", e)
        return None



# =======================# new compare weight API End========================

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
