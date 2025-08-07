from flask import session, redirect, url_for,  request, render_template,flash,jsonify,make_response
#from route import app
from datetime import datetime
from werkzeug.utils import secure_filename
import wheel
import pandas
import os
import json  
from Execute import queries,middleware,responses

def getallrole():
     if request.method == 'POST':
        try:
            data=queries.getallrole()
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)


def getAlluser():
     if request.method == 'POST':
        try:
            data=queries.getAlluser()
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)



# def saveuser():
#         try:
#                 if request.method == 'POST':
                        
#                         formData = request.get_json()
                
#                         formlist=(formData['s_login_id'],formData['s_email'],formData['s_password'],formData['s_dep'],formData['s_contact_no'],formData['s_active'],formData['s_role'],formData['s_created_by'])
#                         id= queries.saveuser(formlist)
#                         if type(id).__name__  != "int":
#                                 if id.json:
#                                         result=id
#                                         status=500
#                         else:
#                                 result=middleware.exs_msgs(id,responses.insert_200,'1020200')
#                                 status=200
#                         return make_response(result,status)
#         except Exception as e:
#                 print("Error in adding area data=================================",e)
#                 return  make_response(middleware.exe_msgs(responses.insert_501,str(e.args),'1020500'),500)

# postgres query

def save_user():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # required_fields = ['name', 'email', 'password', 'created_by']
            # missing = [f for f in required_fields if f not in formData]

            # if missing:
            #     return make_response(
            #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
            #         400
            #     )

            formlist = (formData['name'],formData['email'],formData['password'], datetime.now(),datetime.now()
            )

            insert_id = queries.save_user(formlist)

            if type(insert_id).__name__ != "int":
                return make_response(insert_id, 500)

            result = middleware.exs_msgs(insert_id, responses.insert_200, '1020200')
            return make_response(result, 200)

    except Exception as e:
        print("Error in save_user:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )



def checkusername():
    if request.method == 'POST':
        try:
            print(request)
            id = request.form['s_login_id']
            data = queries.checkusername(id)
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in checking usename =============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)

def updateuser():
    if request.method == 'POST':
        try:
            print(request)
            formData = request.get_json()
            if formData['s_password']=='':
                finaldata=(formData['s_login_id'],formData['s_email'],formData['s_dep'],formData['s_contact_no'],formData['s_active'],formData['s_role'],formData['n_user_id'])
            else:
                finaldata=(formData['s_login_id'],formData['s_email'],formData['s_password'],formData['s_dep'],formData['s_contact_no'],formData['s_active'],formData['s_role'],formData['n_user_id'])
            data=queries.updateuser(finaldata)
            if type(data).__name__  != "str":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.update_200,'1021200')
                status=200
            return make_response(result,status)
        except Exception as e:
            print("Error in updating area data=============================", e)
            return  make_response(middleware.exe_msgs(responses.update_501,str(e.args),'1021500'),500)


def deleteuserById():
    if request.method == 'POST':
        try:
            print(request)
            id = request.form['n_user_id']
            result=queries.deleteuserById(id)
            if type(result).__name__  != "str":
                    if result.json:
                            result=result
                            status=500
            else:
                    result=middleware.exs_msgs(result,responses.delete_200,'1024200')
                    status=200
            return make_response(result,status)
            
        except Exception as e:
            print("Error in deleting area data=============================", e)
            return  make_response(middleware.exe_msgs(responses.delete_501,str(e.args),'1024500'),500)

def getAllUserById():
     if request.method == 'POST':
        try:
            id = request.form['n_user_id']
            data=queries.getAllUserById(id)
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting area data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)


            

            