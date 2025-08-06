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
# def login(data):
#      try:
#           sql="select * FROM tbl_user_master WHERE s_email =%s and s_password =md5(%s) and s_active=1"
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in Login query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# postgres query

def login_user(data):
    try:
        sql = "SELECT id, s_name, s_email FROM tbl_user_master WHERE s_email = %s AND s_password = md5(%s)"
        return executeSql.ExecuteAllNew(sql, data)
    except Exception as e:
        print("Error in login_user query==========================", e)
        return middleware.exe_msgs(responses.queryError_501, str(e.args), '1020320')


#================================For Area Master====================================================

# getting area data
# def getAllArea():
#      try:
#           sql="SELECT *,getStateName(s_state_id) as s_state_name from tbl_area_master"
#           data=''
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingAreaRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

# #saving Area 
# def savearea(data):
#      try:
#           sql="INSERT INTO tbl_area_master(s_state_id,s_area_name,s_short_code,s_incharge,s_created_by) values(%s,%s,%s,%s,%s)"
#           msgs=executeSql.ExecuteReturnId(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in save areaRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')


# #getting area data by id
# def getAllAreaById(id):
#      try:
#           sql="select * from tbl_area_master where n_area_id=%s"
#           data={id}
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in geeting area by id query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')


# #update area data
# def updatearea(data):
#      try:
#           sql="UPDATE tbl_area_master SET s_state_id=%s,s_area_name=%s,s_short_code=%s,s_incharge=%s,s_updated_by=%s WHERE n_area_id=%s"
#           msgs=executeSql.ExecuteOne(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in update areaRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1021310')

# # delete area data
# def deleteareaById(id):
#      try:
#           sql="delete from tbl_area_master where n_area_id=%s"
#           data={id}
#           msgs=executeSql.ExecuteOne(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in deleting area data query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1024310')

# # getting area data by state id
# def getAreaByState(id):
#      try:
#           sql="SELECT * from tbl_area_master where s_state_id=%s"
#           data={id}
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingAreaRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#================================End area Master====================================================

#================================ User Master START ===============================================
# getting role data
# def getallrole():
#      try:
#           sql="SELECT * FROM tbl_role_master"
#           data=''
#           msgs=executeSql.ExecuteAllNew(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in getingroleRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1023310')

#save user data
# def saveuser(data):
#      try:
#           sql="INSERT INTO tbl_user_master(s_login_id,s_email,s_password,s_dep,s_contact_no,s_active,s_role,s_created_by) values(%s,%s,md5(%s),%s,%s,%s,%s,%s)"
#           msgs=executeSql.ExecuteReturnId(sql,data)
#           return msgs
#      except Exception as e:
#           print("Error in save areaRecord query==========================",e)
#           return middleware.exe_msgs(responses.queryError_501,str(e.args),'1020310')
     

# postgres query
def save_user(data):
    try:
        sql = " INSERT INTO tbl_users (name, email, password, created_by, created_date, updated_date) VALUES (%s, %s, md5(%s), %s, %s, %s)"
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

def forgetpassword(id):
     try:
          sql="SELECT * FROM tbl_user where s_email=%s"
          data={id}
          msgs=executeSql.ExecuteAllNew(sql,data)
          return msgs
     except Exception as e:
          print("Error in geeting area by id query==========================",e)
          return middleware.exe_msgs(responses.queryError_501,str(e.args),'1022310')
     

   

   












































   






     

     

     

     



     
     

   
              

      


            

     

     

     




     
     

     

     
 
     
  
     
  
     

   

 

 

 
# end    
           