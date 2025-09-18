from flask import session, redirect, url_for, Response,  request, render_template,flash,jsonify,make_response
#from route import app
from datetime import  date, datetime, time 
from werkzeug.utils import secure_filename
import wheel
import pandas
from Execute.Functions.pdf_parser import process_pdf
import numpy as np
import os
import json 
from scipy.optimize import newton 
from email.utils import parsedate_to_datetime   # <-- new import
from Execute import queries,middleware,responses

# def getallrole():
#      if request.method == 'POST':
#         try:
#             data=queries.getallrole()
#             if type(data).__name__  != "list":
#                 if data.json:
#                     result=data
#                     status=500
#             else:
#                 result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
#                 status=200
                        
#             return make_response(result,status)
#         except Exception as e:
#             print("Error in getting role data=============================", e)
#             return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)


# def getAlluser():
#      if request.method == 'POST':
#         try:
#             data=queries.getAlluser()
#             if type(data).__name__  != "list":
#                 if data.json:
#                     result=data
#                     status=500
#             else:
#                 result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
#                 status=200
                        
#             return make_response(result,status)
#         except Exception as e:
#             print("Error in getting role data=============================", e)
#             return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)

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




#========================================Entity Table Start ====================================================
# def entity_table():
#     try:
#         if request.method == 'POST':
#             formData = request.get_json()

#             # required_fields = ['scrip_name', 'scripcode', 'entityID', 'benchmark',category, sector,  nickname]
#             # missing = [f for f in required_fields if f not in formData]
#             # scrip_name, scripcode, entityID, benchmark, category, sector, nickname, created_at

#             # if missing:
#             #     return make_response(
#             #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
#             #         400
#             #     )

#             next_id = queries.get_next_entity_id()
#             entity_id = f"ENT-{str(next_id).zfill(4)}"  # ENT-0001 style

#             formlist = (formData['scrip_name'],formData['scripcode'],entity_id,formData['benchmark'],formData['category'],formData['sector'],formData['nickname'], datetime.now())

#             insert_id = queries.entity_table(formlist)

#             if type(insert_id).__name__ != "int":
#                 return make_response(insert_id, 500)

#             result = middleware.exs_msgs(insert_id, responses.insert_200, '1020200')
#             return make_response(result, 200)

#     except Exception as e:
#         print("Error in save_user:", e)
#         return make_response(
#             middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
#             500
#         )

def entity_table():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            formlist = (formData['scripname'],formData.get('scripcode'),None,formData.get('benchmark'),formData['category'],formData.get('subcategory'),formData.get('nickname'),datetime.now(),formData.get('isin')
            )

            insert_id = queries.entity_table(formlist)

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
    
def getAllentity():
     if request.method == 'GET':
        try:
            data=queries.getAllentity()
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
        

def getAllMutualFund():
     if request.method == 'GET':
        try:
            data=queries.getAllMutualFund()
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
  

        


def getMutualFundDataById():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getMutualFundDataById(entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )


                
def update_entity_table():
    try:
        if request.method == 'PUT': 
            formData = request.get_json()

           
            formlist = (formData.get('scripname'),
                        formData.get('scripcode'),
                        formData.get('benchmark'),
                        formData.get('category'),
                        formData.get('subcategory'),
                        formData.get('nickname'),
                        formData.get('isin'),
                        datetime.now(),
                        formData.get('id')
            )

            updated_rows = queries.update_entity_table(formlist)

            if type(updated_rows).__name__ != "int":
                return make_response(updated_rows, 500)

            if updated_rows == 0:
                return make_response(
                    middleware.exe_msgs(responses.update_404, "No record found to update", '1020404'),
                    404
                )

            result = middleware.exs_msgs(updated_rows, responses.update_200, '1020400')
            return make_response(result, 200)

    except Exception as e:
        print("Error in update_entity_table:", e)
        return make_response(
            middleware.exe_msgs(responses.update_501, str(e.args), '1020501'),
            500
        )
    

    
def delete_entity():
    try:
        entity_id = None

        # If DELETE → get from query parameters
        if request.method == 'DELETE':
            entity_id = request.args.get('id')

        # If POST → get from form-data or JSON
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('id')
            else:
                entity_id = request.form.get('id')

        # Validate input
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.delete_501, "Missing id parameter", '1024501'),
                400
            )

        # Perform deletion
        deleted_rows = queries.DeleteEntityByid(entity_id)

        if isinstance(deleted_rows, int):
            if deleted_rows > 0:
                result = middleware.exs_msgs(deleted_rows, responses.delete_200, '1024200')
                status = 200
            else:
                result = middleware.exe_msgs(responses.delete_404, "No record found to delete", '1024504')
                status = 404
        else:
            # Query returned error message object
            result = deleted_rows
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in delete_entity:", e)
        return make_response(
            middleware.exe_msgs(responses.delete_501, str(e.args), '1024500'),
            500
        )


def getCountOfAllEntity():
     if request.method == 'GET':
        try:
            data=queries.getCountOfAllEntity()
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



#========================================Entity Table End ======================================================



#========================================Action Table Start ====================================================
def action_table():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # required_fields = ['scrip_code', 'mode', 'order_type', 'scrip_name',isin, order_number, folio_number, nav, stt, unit, redeem_amount, purchase_amount, cgst, sgst, igst, ugst, stamp_duty, cess_value, net_amount]
            # missing = [f for f in required_fields if f not in formData]

            # if missing:
            #     return make_response(
            #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
            #         400
            #     )

            formlist = (formData['scrip_code'],formData['mode'],formData['order_type'],formData['scrip_name'],formData['isin'],formData['order_number'],formData['folio_number'],formData['nav'],formData['stt'],formData['unit'],formData['redeem_amount'],formData['purchase_amount'],formData['cgst'],formData['sgst'],formData['igst'],formData['ugst'],formData['stamp_duty'],formData['cess_value'],formData['net_amount'], datetime.now(),formData['entityid'],formData['purchase_value'],formData['order_date'],formData['sett_no']
            )

            insert_id = queries.action_table(formlist)

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
    

def getAllAction():
     if request.method == 'GET':
        try:
            data=queries.getAllAction()
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

# def getActionByentId():
#     if request.method == 'GET':
#         try:
#             # If JSON body
#             if request.is_json:
#                 entity_id = request.json.get('entityid')
#             else:
#                 entity_id = request.form.get('entityid')  # for form-data

#             if not entity_id:
#                 return make_response(
#                     middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
#                     400
#                 )

#             data = queries.getActionByentId(entity_id)

#             if isinstance(data, list):
#                 result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
#                 status = 200
#             else:
#                 result = data
#                 status = 500

#             return make_response(result, status)

#         except Exception as e:
#             print("Error in getting underlying by id:", e)
#             return make_response(
#                 middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
#                 500
#             )        

def serialize_dates(data):
    for row in data:
        for field, value in row.items():
            if value is None:
                continue

            # Format date, datetime, or time objects
            if isinstance(value, (datetime, date)):
                # row[field] = value.strftime("%Y-%m-%d")
                row[field] = value.strftime("%d-%m-%Y")
            elif isinstance(value, time):
                row[field] = value.strftime("%H:%M:%S")
    return data


def  getActionByentId():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getActionByentId(entity_id)

        # Return proper response
        if isinstance(data, list):
            data = serialize_dates(data)
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )





def getMfByentId():
    if request.method == 'GET':
        try:
            data = queries.getMfByentId()

            if type(data).__name__ != "list":
                if data.json:
                    result = data
                    status = 500
            else:
                # ✅ Auto-detect and format all date fields
                data = serialize_dates(data)

                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
                        
            return make_response(result, status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return make_response(
                middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
                500
            )
        

def insertMFNavData():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            formlist = (formData['entityid'],formData['nav'],formData['nav_date'], datetime.now()
            )

            insert_msg = queries.insert_MF_NavData(formlist)

            
            return make_response(
                middleware.exs_msgs(insert_msg, responses.insert_200, '1020200'),
                200 )

    except Exception as e:
        print("Error in insertNavData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )
   

def  getAllMutualFundNav():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getAll_Mutual_Fund_Nav(entity_id)

        # Return proper response
        if isinstance(data, list):
            data = serialize_dates(data)
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )






#========================================Action Table End ======================================================

#========================================mcap Start ====================================================
def mcap_table():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # required_fields = ['company_name', 'sector', 'symbol', 'series',isin_code]
            # missing = [f for f in required_fields if f not in formData]

            # if missing:
            #     return make_response(
            #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
            #         400
            #     )

            formlist = (formData['company_name'],formData['sector'],formData['symbol'],formData['series'],formData['isin_code'], datetime.now()
            )

            insert_id = queries.mcap_table(formlist)

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

#========================================mcap Table End ======================================================


#========================================Underlying Start ====================================================
# def underlying_table():
#     try:
#         if request.method == 'POST':
#             formData = request.get_json()

#             # required_fields = ['company_name', 'sector', 'symbol', 'series',isin_code]
#             # missing = [f for f in required_fields if f not in formData]

#             # if missing:
#             #     return make_response(
#             #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
#             #         400
#             #     )

#             formlist = (formData['company_name'],formData['scripcode'],formData['weightage'],formData['sector'],formData['isin_code'], datetime.now(),formData['entityid'],
#             )

#             insert_id = queries.underlying_table(formlist)

#             if type(insert_id).__name__ != "int":
#                 return make_response(insert_id, 500)

#             result = middleware.exs_msgs(insert_id, responses.insert_200, '1020200')
#             return make_response(result, 200)

#     except Exception as e:
#         print("Error in save_user:", e)
#         return make_response(
#             middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
#             500
#         )
    
def underlying_table():
    try:
        if request.method != 'POST':
            return make_response(
                middleware.exe_msgs(responses.insert_501, "Invalid request method. Use POST.", '1020503'),
                405
            )

        formData = request.get_json()
        if not formData:
            return make_response(
                middleware.exe_msgs(responses.insert_501, "Request body is empty.", '1020502'),
                400
            )

        inserted_ids = []

        # Check if it's multiple rows (has 'rows' key)
        if 'rows' in formData:
            entityid = formData.get('entityid')
            rows = formData.get('rows')

            if not entityid or not isinstance(rows, list) or len(rows) == 0:
                return make_response(
                    middleware.exe_msgs(responses.insert_501, "Missing or invalid 'entityid' or 'rows'.", '1020501'),
                    400
                )

        else:
            # Single row case
            entityid = formData.get('entityid')
            rows = [formData]  # wrap single row as list

            if not entityid:
                return make_response(
                    middleware.exe_msgs(responses.insert_501, "Missing 'entityid' in request.", '1020501'),
                    400
                )

        # Loop through rows and insert
        for row in rows:
            formlist = (
                row.get('company_name'),
                row.get('scripcode'),
                float(row.get('weightage') or 0),
                row.get('sector'),
                row.get('isin_code'),
                datetime.now(),
                entityid,
                row.get('tag')
            )

            insert_id = queries.underlying_table(formlist)

            if not isinstance(insert_id, int):
                return make_response(insert_id, 500)

            inserted_ids.append(insert_id)

        result = middleware.exs_msgs(inserted_ids, responses.insert_200, '1020200')
        return make_response(result, 200)

    except Exception as e:
        print("Error in underlying_table:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )

def getAllUnderlying():
     if request.method == 'GET':
        try:
            data=queries.getAllUnderlying()
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
        

# def getUnderlyingById():
#      if request.method == 'POST':
#         try:
#             entity_id = request.form['entityid']
#             data=queries.getUnderlyingById(entity_id)
#             if type(data).__name__  != "list":
#                 if data.json:
#                     result=data
#                     status=500
#             else:
#                 result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
#                 status=200
                        
#             return make_response(result,status)
#         except Exception as e:
#             print("Error in getting area data=============================", e)
#             return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)        


# def getUnderlyingById():
#     if request.method == 'POST':
#         try:
#             # If JSON body
#             if request.is_json:
#                 entity_id = request.json.get('entityid')
#             else:
#                 entity_id = request.form.get('entityid')  # for form-data

#             if not entity_id:
#                 return make_response(
#                     middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
#                     400
#                 )

#             data = queries.getUnderlyingById(entity_id)

#             if isinstance(data, list):
#                 result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
#                 status = 200
#             else:
#                 result = data
#                 status = 500

#             return make_response(result, status)

#         except Exception as e:
#             print("Error in getting underlying by id:", e)
#             return make_response(
#                 middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
#                 500
#             )

def  getUnderlyingById():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getUnderlyingById(entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )



def getUnderlyingByMf():
     if request.method == 'GET':
        try:
            data=queries.getUnderlyingByMf()
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



    
# def ClearUnderlyingdata():
#     try:
#         entity_id = None

#         # If DELETE → get from query parameters
#         if request.method == 'DELETE':
#             entity_id = request.args.get('entityid')

#         # If POST → get from form-data or JSON
#         elif request.method == 'POST':
#             if request.is_json:
#                 entity_id = request.json.get('id')
#             else:
#                 entity_id = request.form.get('id')

#         # Validate input
#         if not entity_id:
#             return make_response(
#                 middleware.exe_msgs(responses.delete_501, "Missing id parameter", '1024501'),
#                 400
#             )

#         # Perform deletion
#         deleted_rows = queries.ClearUnderlyingdata(entity_id)

#         if isinstance(deleted_rows, int):
#             if deleted_rows > 0:
#                 result = middleware.exs_msgs(deleted_rows, responses.delete_200, '1024200')
#                 status = 200
#             else:
#                 result = middleware.exe_msgs(responses.delete_404, "No record found to delete", '1024504')
#                 status = 404
#         else:
#             # Query returned error message object
#             result = deleted_rows
#             status = 500

#         return make_response(result, status)

#     except Exception as e:
#         print("Error in delete_entity:", e)
#         return make_response(
#             middleware.exe_msgs(responses.delete_501, str(e.args), '1024500'),
#             500
 
#       )

def ClearUnderlyingdata():
    try:
        entity_id = None

        # If DELETE → get from query parameters
        if request.method == 'DELETE':
            entity_id = request.args.get('entityid')

        # If POST → get from form-data or JSON
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate input
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.delete_501, "Missing entityid parameter", '1024501'),
                400
            )

        # Perform deletion or insertion
        result_summary = queries.ClearUnderlyingdata(entity_id)

        if result_summary["action"] == "deleted":
            result = middleware.exs_msgs(result_summary, responses.delete_200, '1024200')
            status = 200

        elif result_summary["action"] == "inserted":
            result = middleware.exs_msgs(result_summary, responses.insert_200, '1024201')
            status = 200

        elif result_summary["action"] == "not_found":
            result = middleware.exe_msgs(responses.delete_404, "No matching entity found", '1024204')
            status = 404

        else:  # error case
            result = middleware.exe_msgs(responses.delete_501, result_summary.get("error", "Unknown error"), '1024500')
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in ClearUnderlyingdata function:", e)
        return make_response(
            middleware.exe_msgs(responses.delete_501, str(e.args), '1024500'),
            500
        )

# new running
# def ClearUnderlyingdata():
#     try:
#         # Get entity_id from DELETE query param or POST JSON/form
#         entity_id = None
#         if request.method == 'DELETE':
#             entity_id = request.args.get('entityid')
#         elif request.method == 'POST':
#             if request.is_json:
#                 entity_id = request.json.get('entityid')
#             else:
#                 entity_id = request.form.get('entityid')

#         if not entity_id:
#             return make_response({
#                 "data": {"message": "Missing entityid parameter", "rows_affected": 0},
#                 "successmsgs": "Failed",
#                 "code": "1024501"
#             }, 400)

#         # Call queries.py function
#         result = queries.ClearUnderlyingdata(entity_id)

#         action = result.get("action")
#         rows_affected = result.get("rows_affected", 0)

#         if action == "deleted":
#             return make_response({
#                 "data": {
#                     "message": f"Entity {entity_id} deleted from tbl_underlying",
#                     "rows_affected": rows_affected
#                 },
#                 "successmsgs": "Record(s) deleted successfully",
#                 "code": "1024200"
#             }, 200)

#         elif action == "inserted":
#             return make_response({
#                 "data": {
#                     "message": f"Entity {entity_id} inserted into tbl_underlying",
#                     "rows_affected": rows_affected
#                 },
#                 "successmsgs": "Inserted Successfully",
#                 "code": "1024201"
#             }, 200)

#         elif action == "not_found":
#             return make_response({
#                 "data": {
#                     "message": f"Entity {entity_id} not found in tbl_entity",
#                     "rows_affected": 0
#                 },
#                 "successmsgs": "No matching entity found",
#                 "code": "1024204"
#             }, 404)

#         else:
#             return make_response({
#                 "errmsgs": f"Query error: {result.get('error')}",
#                 "error": result.get('error'),
#                 "code": "1024503"
#             }, 500)

#     except Exception as e:
#         import traceback
#         error_details = traceback.format_exc()
#         print("🔥 ClearUnderlyingdata API failed:", error_details)
#         return make_response({
#             "errmsgs": f"Internal Server Error: {str(e)}",
#             "error": error_details,
#             "code": "1024503"
#         }, 500)

# new

#========================================Underlying Table End ======================================================

            
#========================================bigsheet Table Start ======================================================
# def getCamByid():
#      if request.method == 'GET':
#         try:
#             data=queries.getCamByid()
#             if type(data).__name__  != "list":
#                 if data.json:
#                     result=data
#                     status=500
#             else:
#                 result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
#                 status=200
                        
#             return make_response(result,status)
#         except Exception as e:
#             print("Error in getting role data=============================", e)
#             return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)   
# 
 
def getCamByid():
    if request.method == 'GET':
        try:
            company_name = request.args.get('company')  # /get_isin?company=HDFC
            data = queries.getCamByid(company_name)

            if type(data).__name__ != "list":
                if data.json:
                    result = data
                    status = 500
            else:
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200

            return make_response(result, status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return make_response(
                middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
                500
            )
#========================================bigsheet Table End ======================================================  




#========================================AIF Table Start ====================================================
def InsertAifData():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # required_fields = ['entityid', 'trans_date', 'trans_type', 'contribution_amount', 'setup_expense', 'stamp_duty', 'amount_invested', 'post_tax_nav', 'num_units', 'balance_units', 'strategy_name', 'amc_name', 'created_at']
            # missing = [f for f in required_fields if f not in formData]

            # if missing:
            #     return make_response(
            #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
            #         400
            #     )

            formlist = (formData['entityid'],formData['trans_date'],formData['trans_type'],formData['contribution_amount'],formData['setup_expense'],formData['stamp_duty'],formData['amount_invested'],formData['post_tax_nav'],formData['num_units'],formData['balance_units'],formData['strategy_name'],formData['amc_name'], datetime.now()
            )

            insert_msg = queries.InsertAifData(formlist)

            # Always return 200 if insert succeeds
            return make_response(
                middleware.exs_msgs(insert_msg, responses.insert_200, '1020200'),
                200
            )

    except Exception as e:
        print("Error in insertNavData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )
   

def insertNavData():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # required_fields = ['entityid', 'trans_date', 'trans_type', 'contribution_amount', 'setup_expense', 'stamp_duty', 'amount_invested', 'post_tax_nav', 'num_units', 'balance_units', 'strategy_name', 'amc_name', 'created_at']
            # missing = [f for f in required_fields if f not in formData]

            # if missing:
            #     return make_response(
            #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
            #         400
            #     )

            formlist = (formData['entityid'],formData['pre_tax_nav'],formData['post_tax_nav'],formData['nav_date'], datetime.now()
            )

            insert_msg = queries.insertNavData(formlist)

            
            return make_response(
                middleware.exs_msgs(insert_msg, responses.insert_200, '1020200'),
                200
            )

    except Exception as e:
        print("Error in insertNavData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )
   



def getAllAif():
     if request.method == 'GET':
        try:
            data=queries.getAllAif()
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


def  getAifActionTablebyId ():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getAifActionTablebyId (entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )





def getAifEntity():
     if request.method == 'GET':
        try:
            data=queries.getAifEntity()
            # if type(data).__name__  != "list":
            #     if data.json:
            #         result=data
            #         status=500
            # else:
            #     result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
            #     status=200
            if isinstance(data, list):
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
            else:
                result = data
                status = 500

            return make_response(result, status)
     
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)            

# def  getAifEntity ():
#     try:
#         entity_id = None

#         # Handle GET → from query params
#         if request.method == 'GET':
#             entity_id = request.args.get('entityid')

#         # Handle POST → from JSON or form-data
#         elif request.method == 'POST':
#             if request.is_json:
#                 entity_id = request.json.get('entityid')
#             else:
#                 entity_id = request.form.get('entityid')

#         if not entity_id:
#             return make_response(
#                 middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
#                 400
#             )

#         data = queries.getAifEntity (entity_id)

#         # Return proper response
#         if isinstance(data, list):
#             result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
#             status = 200
#         else:
#             result = data
#             status = 500

#         return make_response(result, status)

#     except Exception as e:
#         print("Error in getting underlying by id:", e)
#         return make_response(
#             middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
#             500
#         )




#========================================AIF Table End ======================================================






#========================================ETF Table Start ====================================================
def InsertEtfData():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # required_fields = ['entityid', 'order_number', 'order_time', 'trade_number', 'trade_time', 'security_description', 'order_type', 'quantity', 'gross_rate', 'trade_price_per_unit', 'brokerage_per_unit', 'created_at']
            # missing = [f for f in required_fields if f not in formData]

            # if missing:
            #     return make_response(
            #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
            #         400
            #     )

            formlist = (formData['entityid'],formData['order_number'],formData['order_time'],formData['trade_number'],formData['trade_time'],formData['security_description'],formData['order_type'],formData['quantity'],formData['gross_rate'],formData['trade_price_per_unit'],formData['brokerage_per_unit'],formData['net_rate_per_unit'],formData['closing_rate'],formData['gst'],formData['stt'],formData['net_total_before_levies'],formData['remarks'], datetime.now(), formData['trade_date'])

            insert_msg = queries.InsertEtfData(formlist)

            # Always return 200 if insert succeeds
            return make_response(
                middleware.exs_msgs(insert_msg, responses.insert_200, '1020200'),
                200
            )

    except Exception as e:
        print("Error in insertNavData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )
   

def getAllEtf():
     if request.method == 'GET':
        try:
            data=queries.getAllEtf()
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


def  getEtfActionTablebyId ():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getEtfActionTablebyId (entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )



def getEtfEntity():
     if request.method == 'GET':
        try:
            data=queries.getEtfEntity()
            # if type(data).__name__  != "list":
            #     if data.json:
            #         result=data
            #         status=500
            # else:
            #     result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
            #     status=200
            if isinstance(data, list):
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
            else:
                result = data
                status = 500

            return make_response(result, status)
     
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)  


#========================================ETF Table End ======================================================



#========================================Commodities Table Start ====================================================
def Insert_CommoditiesDirect():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # Prepare tuple in same order as SQL
            formlist = (formData.get('entityid'),formData.get('contract_note_number'),formData.get('trade_date'),formData.get('client_code'),formData.get('client_name'),formData.get('order_number'),formData.get('order_time'),formData.get('trade_number'),formData.get('description'),formData.get('order_type'),formData.get('qty'),formData.get('trade_price'),formData.get('brokerage_per_unit', 0),formData.get('net_rate_per_unit'),formData.get('gst', 0),formData.get('stt', 0),formData.get('security_transaction_tax', 0),formData.get('exchange_transaction_charges', 0),formData.get('sebi_turnover_fees', 0),formData.get('stamp_duty', 0),formData.get('ipft', 0),formData.get('net_total'),formData.get('net_amount_receivable'),datetime.now())

            insert_id = queries.Insert_CommoditiesDirect(formlist)

            # If insert_id is None, treat as success
            if insert_id is None:
                insert_id = 0

            try:
                result = middleware.exs_msgs(insert_id, responses.insert_200, '1020200')
            except Exception as e:
                print("Error in middleware.exs_msgs:", e)
                # Return minimal success response
                result = {"status": "success", "insert_id": insert_id}

            return make_response(result, 200)

    except Exception as e:
        print("Error in Insert_directData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e), '1020500'),
            500
        )

   

def getAllDirectEquityCommodities():
     if request.method == 'GET':
        try:
            data=queries.getAllDirectEquityCommodities()
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


def  getCommoditiesActionTablebyId ():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getCommoditiesActionTablebyId (entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )
    

def getDEDetailCommoditiesEntityById():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getDEDetailCommoditiesEntityById(entity_id)

        # Return proper response
        if isinstance(data, list):
            data = serialize_dates(data)
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )    



def getCommoditiesEntity():
     if request.method == 'GET':
        try:
            data=queries.getCommoditiesEntity()
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



def getCountOfAllCommodities():
     if request.method == 'GET':
        try:
            data=queries.getCountOfAllCommodities()
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




def getAllCommoditiesInstrument():
    if request.method == 'GET':
        try:
            data = queries.getAllCommoditiesInstrument()

            result = {
                "code": "1023200",
                "successmsgs": responses.getAll_200,
                "data": data
            }
            return make_response(result, 200)

        except Exception as e:
            print("Error in getAllActionInstrument =============================", e)
            return make_response(
                middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
                500
            )                         



#========================================Commodities Table End ======================================================



#======================================== PMS Cliend and AMC Table Start ====================================================
def insertClientAction():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            formlist = (formData['entityid'],formData['order_type'],formData['trade_price'],formData['cheque'], datetime.now(),formData['trade_date'])

            insert_msg = queries.insertClientAction(formlist)

            # Always return 200 if insert succeeds
            return make_response(
                middleware.exs_msgs(insert_msg, responses.insert_200, '1020200'),
                200
            )

    except Exception as e:
        print("Error in insertNavData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )
   

def getAllPmsClientActionTable():
     if request.method == 'GET':
        try:
            data=queries.getAllPmsClientActionTable()
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


def  getPmsClientActionById ():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getPmsClientActionById (entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )



def getPmsClientEntity():
     if request.method == 'GET':
        try:
            data=queries.getPmsClientEntity()
            # if type(data).__name__  != "list":
            #     if data.json:
            #         result=data
            #         status=500
            # else:
            #     result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
            #     status=200
            if isinstance(data, list):
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
            else:
                result = data
                status = 500

            return make_response(result, status)
     
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)


def  getPmsEquityDetailbyId ():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getPmsEquityDetailbyId (entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )

        

#################-> PMS Amc ###################
def insertPmsAmcAction():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            formlist = (formData['entityid'],formData['security_description'],formData['order_type'],formData['quantity'],formData['trade_price'],formData['net_amount'],
                datetime.now()
            )

            insert_msg = queries.insertPmsAmcAction(formlist)

            # ✅ Check if insert actually succeeded
            if not insert_msg or (isinstance(insert_msg, int) and insert_msg <= 0):
                return make_response(
                    middleware.exe_msgs(
                        responses.insert_501,
                        f"Insert failed: entityid {formData['entityid']} may not exist in tbl_entity",
                        '1020502'
                    ),
                    400
                )

            return make_response(
                middleware.exs_msgs(insert_msg, responses.insert_200, '1020200'),
                200
            )

    except Exception as e:
        print("Error in insertPmsAmcAction:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )

   

def getAllPmsAmcActionTable():
     if request.method == 'GET':
        try:
            data=queries.getAllPmsAmcActionTable()
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


def  getPmsAmcActionById ():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getPmsAmcActionById (entity_id)

        # Return proper response
        if isinstance(data, list):
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )



def getPmsAmcEntity():
     if request.method == 'GET':
        try:
            data=queries.getPmsAmcEntity()
            # if type(data).__name__  != "list":
            #     if data.json:
            #         result=data
            #         status=500
            # else:
            #     result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
            #     status=200
            if isinstance(data, list):
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
            else:
                result = data
                status = 500

            return make_response(result, status)
     
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)  


#========================================PMS Cliend and AMC Table Start ======================================================






#========================================Direct Table Start ====================================================
# def Insert_directData():
#     try:
#         if request.method == 'POST':
#             formData = request.get_json()

#             # required_fields = ['entityid', 'trans_date', 'trans_type', 'contribution_amount', 'setup_expense', 'stamp_duty', 'amount_invested', 'post_tax_nav', 'num_units', 'balance_units', 'strategy_name', 'amc_name', 'created_at']
#             # missing = [f for f in required_fields if f not in formData]

#             # if missing:
#             #     return make_response(
#             #         middleware.exe_msgs(responses.insert_501, f"Missing fields: {', '.join(missing)}", '1020501'),
#             #         400
#             #     )

#             formlist = (formData['entityid'],formData['contract_note_number'],formData['trade_date'],formData['client_code'],formData['client_name'],formData['order_number'],formData['order_time'],formData['trade_number'],formData['description'],formData['order_type'],formData['qty'],formData['trade_price'],formData['brokerage_per_unit'],formData['net_rate_per_unit'],formData['gst'],formData['stt'],formData['security_transaction_tax'],formData['exchange_transaction_charges'],formData['sebi_turnover_fees'],formData['stamp_duty'],formData['ipft'],formData['net_total'],formData['net_amount_receivable'],datetime.now()
#             )

#             insert_id = queries.Insert_directData(formlist)

#             if type(insert_id).__name__ != "int":
#                 return make_response(insert_id, 500)

#             result = middleware.exs_msgs(insert_id, responses.insert_200, '1020200')
#             return make_response(result, 200)

#     except Exception as e:
#         print("Error in save_user:", e)
#         return make_response(
#             middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
#             500
#         )
   
def Insert_directData():
    try:
        if request.method == 'POST':
            formData = request.get_json()

            # Prepare tuple in same order as SQL
            formlist = (
                formData.get('entityid'),
                formData.get('contract_note_number'),
                formData.get('trade_date'),
                formData.get('client_code'),
                formData.get('client_name'),
                formData.get('order_number'),
                formData.get('order_time'),
                formData.get('trade_number'),
                formData.get('description'),
                formData.get('order_type'),
                formData.get('qty'),
                formData.get('trade_price'),
                formData.get('brokerage_per_unit', 0),
                formData.get('net_rate_per_unit'),
                formData.get('gst', 0),
                formData.get('stt', 0),
                formData.get('security_transaction_tax', 0),
                formData.get('exchange_transaction_charges', 0),
                formData.get('sebi_turnover_fees', 0),
                formData.get('stamp_duty', 0),
                formData.get('ipft', 0),
                formData.get('net_total'),
                formData.get('net_amount_receivable'),
                datetime.now()
            )

            insert_id = queries.Insert_directData(formlist)

            # If insert_id is None, treat as success
            if insert_id is None:
                insert_id = 0

            try:
                result = middleware.exs_msgs(insert_id, responses.insert_200, '1020200')
            except Exception as e:
                print("Error in middleware.exs_msgs:", e)
                # Return minimal success response
                result = {"status": "success", "insert_id": insert_id}

            return make_response(result, 200)

    except Exception as e:
        print("Error in Insert_directData:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e), '1020500'),
            500
        )


def getallDirectdata():
     if request.method == 'GET':
        try:
            data=queries.getallDirectdata()


            if not isinstance(data, list):
                 result = data
                 status = 500
            else:
                data = serialize_dates(data)
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)  


def  getdirectByentId():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        data = queries.getdirectByentId(entity_id)

        # Return proper response
        if isinstance(data, list):
            data = serialize_dates(data)
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )

def getAllActionTableOfDirectEquity():
     if request.method == 'GET':
        try:
            data=queries.getAllActionTableOfDirectEquity()
            if isinstance(data, list):
                data = serialize_dates(data)
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
            else:
                result=data
                status=500
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500) 



def getDirectEquityByid():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getDirectEquityByid(entity_id)

        # Return proper response
        if isinstance(data, list):
            data = serialize_dates(data)
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )
    
def getDEDetailActionTable():
    try:
        entity_id = None

        # Handle GET → from query params
        if request.method == 'GET':
            entity_id = request.args.get('entityid')

        # Handle POST → from JSON or form-data
        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate entity_id
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.getAll_501, "Missing entityid parameter", '1023501'),
                400
            )

        # Query the database
        data = queries.getDEDetailActionTable(entity_id)

        # Return proper response
        if isinstance(data, list):
            data = serialize_dates(data)
            result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
            status = 200
        else:
            result = data
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in getting underlying by id:", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
            500
        )
     




#========================================Direct Table End ======================================================



# ======================================Delete Etity data From Underlying,Action Table======================================
# def delete_entity_data():
#     try:
#         entity_id = None

#         # If DELETE → get from body
#         if request.method == 'DELETE':
#             data = request.get_json()
#             entity_id = data.get("entityid")

#         # If POST → get from form-data or JSON
#         elif request.method == 'POST':
#             if request.is_json:
#                 entity_id = request.json.get('entityid')
#             else:
#                 entity_id = request.form.get('entityid')

#         # Validate input
#         if not entity_id:
#             return make_response(
#                 middleware.exe_msgs(responses.delete_501, "Missing entityid parameter", '1024501'),
#                 400
#             )

#         # Perform deletion
#         deleted_summary = queries.delete_entity_data(entity_id)

#         if isinstance(deleted_summary, dict):
#             total_deleted = sum(deleted_summary.values())
#             if total_deleted > 0:
#                 result = middleware.exs_msgs(
#                     {"total_deleted": total_deleted, "breakdown": deleted_summary},
#                     responses.delete_200,
#                     '1024200'
#                 )
#                 status = 200
#             else:
#                 result = middleware.exe_msgs(
#                     responses.delete_404,
#                     f"No record found to delete for entityid {entity_id}",
#                     '1024504'
#                 )
#                 status = 404
#         else:
#             # Query returned error object
#             result = deleted_summary
#             status = 500

#         return make_response(result, status)

#     except Exception as e:
#         print("Error in delete_entity:", e)
#         return make_response(
#             middleware.exe_msgs(responses.delete_501, str(e.args), '1024500'),
#             500
#         )

def delete_entity_data():
    try:
        entity_id = None

        if request.method == 'DELETE':
            # Try JSON body first
            if request.is_json:
                data = request.get_json()
                entity_id = data.get("entityid")

            # If not found, fallback to query param
            if not entity_id:
                entity_id = request.args.get("entityid")

        elif request.method == 'POST':
            if request.is_json:
                entity_id = request.json.get('entityid')
            else:
                entity_id = request.form.get('entityid')

        # Validate input
        if not entity_id:
            return make_response(
                middleware.exe_msgs(responses.delete_501, "Missing entityid parameter", '1024501'),
                400
            )

        # Perform deletion
        deleted_summary = queries.delete_entity_data(entity_id)

        if isinstance(deleted_summary, dict):
            total_deleted = sum(deleted_summary.values())
            if total_deleted > 0:
                result = middleware.exs_msgs(
                    {"total_deleted": total_deleted, "breakdown": deleted_summary},
                    responses.delete_200,
                    '1024200'
                )
                status = 200
            else:
                result = middleware.exe_msgs(
                    responses.delete_404,
                    f"No record found to delete for entityid {entity_id}",
                    '1024504'
                )
                status = 404
        else:
            result = deleted_summary
            status = 500

        return make_response(result, status)

    except Exception as e:
        print("Error in delete_entity:", e)
        return make_response(
            middleware.exe_msgs(responses.delete_501, str(e.args), '1024500'),
            500
        )

# ======================================Delete Etity data From Underlying,Action Table======================================


# ======================================Get All Equity======================================

def getAllEquity():
     if request.method == 'GET':
        try:
            data=queries.getAllEquity()


            if not isinstance(data, list):
                 result = data
                 status = 500
            else:
                data = serialize_dates(data)
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500) 
        
# ======================================Get All Equity======================================



# ======================================Get All EquityActionTable======================================

def getEquityActionTable():
     if request.method == 'GET':
        try:
            data=queries.getEquityActionTable()


            if not isinstance(data, list):
                 result = data
                 status = 500
            else:
                data = serialize_dates(data)
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500) 
        
# ======================================Get All EquityActionTable======================================


# ======================================== Get allMfEquityUnderlyingCount Start============================

def GetallMfEquityUnderlyingCount():
     if request.method == 'GET':
        try:
            data=queries.GetallMfEquityUnderlyingCount()


            if not isinstance(data, list):
                 result = data
                 status = 500
            else:
                data = serialize_dates(data)
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500) 


# ======================================== Get allMfEquityUnderlyingCount End============================

# ======================================Get All Home Dtata of Equity======================================

def getAllHomeData():
     if request.method == 'GET':
        try:
            data=queries.getAllHomeData()


            if not isinstance(data, list):
                 result = data
                 status = 500
            else:
                data = serialize_dates(data)
                result = middleware.exs_msgs(data, responses.getAll_200, '1023200')
                status = 200
                        
            return make_response(result,status)
        except Exception as e:
            print("Error in getting role data=============================", e)
            return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500) 
        
# ======================================Get All Home Dtata of Equity======================================





# ======================================Get All Action table Instrument======================================

def getAllActionInstrument():
    if request.method == 'GET':
        try:
            data = queries.getAllActionInstrument()

            result = {
                "code": "1023200",
                "successmsgs": responses.getAll_200,
                "data": data
            }
            return make_response(result, 200)

        except Exception as e:
            print("Error in getAllActionInstrument =============================", e)
            return make_response(
                middleware.exe_msgs(responses.getAll_501, str(e.args), '1023500'),
                500
            )

# ======================================Get All Action table Instrument======================================




# ======================================calculate Xirr (IRR)======================================


# Calculate XIRR
# -------------------- Core IRR Calculation --------------------
# import numpy as np

def calculate_xirr(cashflows, dates, guess=0.1):
    """
    Calculate XIRR (annualized IRR) that can be positive or negative.
    cashflows: list of amounts (negative for investments, positive for redemptions)
    dates: list of datetime objects corresponding to cashflows
    """
    if not cashflows or not dates or len(cashflows) != len(dates):
        return None

    # Convert dates to year fractions relative to first date
    days = np.array([(d - dates[0]).days for d in dates], dtype=float)
    years = days / 365.0
    amounts = np.array(cashflows, dtype=float)

    # Define NPV and derivative
    def npv(rate):
        return np.sum(amounts / (1 + rate) ** years)

    def d_npv(rate):
        return np.sum(-years * amounts / (1 + rate) ** (years + 1))

    # Newton-Raphson iteration
    rate = guess
    for _ in range(100):
        f_value = npv(rate)
        f_derivative = d_npv(rate)

        if abs(f_value) < 1e-6:  # converged
            return round(rate * 100, 2)

        if f_derivative == 0:
            break

        rate -= f_value / f_derivative

    # Fallback: numpy.irr (may be deprecated in new numpy versions)
    try:
        irr = np.irr(amounts)
        if irr is not None:
            return round(irr * 100, 2)
    except Exception:
        pass

    return None


def format_irr_response(cashflows, dates):
    if not cashflows or not dates:
        return {
            "annualized_irr_percent": 0.0,
            "simple_return_percent": 0.0,
            "total_invested": 0,
            "total_redemption": 0
        }

    irr = calculate_xirr(cashflows, dates)

    total_invested = -sum(cf for cf in cashflows if cf < 0)  # outflows
    total_redemption = sum(cf for cf in cashflows if cf > 0)  # inflows

    # Simple return = (total inflow - total outflow) / total outflow
    simple_return = None
    if total_invested > 0:
        simple_return = ((total_redemption - total_invested) / total_invested) * 100

    return {
        "annualized_irr_percent": irr if irr is not None else 0.0,
        "simple_return_percent": round(simple_return, 2) if simple_return is not None else 0.0,
        "total_invested": round(total_invested, 2),
        "total_redemption": round(total_redemption, 2)
    }

# -------------------- Endpoints --------------------
def getActionIRR():
    entityid = request.args.get("entityid", "").strip()
    if not entityid:
        return make_response({"error": "entityid is required"}, 400)

    cashflows, dates = queries.get_cashflows_action(entityid)
    if not cashflows:
        return make_response({"error": f"No rows found for entityid={entityid} in tbl_action_table"}, 404)

    response = format_irr_response(cashflows, dates)
    response["entityid"] = entityid
    return make_response({"code": "1023200", **response, "successmsgs": "Fetching Successfully"}, 200)


def getDirectEquityIRR():
    entityid = request.args.get("entityid", "").strip()
    if not entityid:
        return make_response({"error": "entityid is required"}, 400)

    cashflows, dates = queries.get_cashflows_direct_equity(entityid)
    if not cashflows:
        return make_response({"error": f"No rows found for entityid={entityid} in tbl_direct_equity"}, 404)

    response = format_irr_response(cashflows, dates)
    response["entityid"] = entityid
    return make_response({"code": "1023200", **response, "successmsgs": "Fetching Successfully"}, 200)


def getAifIRR():
    entityid = request.args.get("entityid", "").strip()
    if not entityid:
        return make_response({"error": "entityid is required"}, 400)

    cashflows, dates = queries.get_cashflows_aif(entityid)
    if not cashflows:
        return make_response({"error": f"No rows found for entityid={entityid} in tbl_aif"}, 404)

    response = format_irr_response(cashflows, dates)
    response["entityid"] = entityid
    return make_response({"code": "1023200", **response, "successmsgs": "Fetching Successfully"}, 200)



def getALLMutualFundActionTableIRR():
    
    cashflows, dates = queries.get_cashflows_All_action()
    if not cashflows:
        return make_response({"error": "No rows found in tbl_action_table"}, 404)

    response = format_irr_response(cashflows, dates)
    return make_response({"code": "1023200", **response, "successmsgs": "Fetching Successfully"}, 200)


def getDirectEquityCommodityIRR():
    entityid = request.args.get("entityid", "").strip()
    if not entityid:
        return make_response({"error": "entityid is required"}, 400)

    cashflows, dates = queries.getDirectEquityCommodityIRR(entityid)
    if not cashflows:
        return make_response({"error": f"No rows found for entityid={entityid} in tbl_direct_equity"}, 404)

    response = format_irr_response(cashflows, dates)
    response["entityid"] = entityid
    return make_response({"code": "1023200", **response, "successmsgs": "Fetching Successfully"}, 200)

# ======================================calculate Xirr (IRR)======================================


# ============================= Auto PDF Read and Insert Into DB =========================
# old corect working code

# def upload_and_save():
#     try:
#         if request.method != "POST":
#             return make_response({"error": "Method not allowed"}, 405)

#         files = request.files.getlist("files")
#         if not files:
#             return make_response({"error": "No files uploaded"}, 400)

#         category = request.form.get("category")
#         subcategory = request.form.get("subcategory")
#         if not category or not subcategory:
#             return make_response({"error": "Category & Subcategory required"}, 400)

#         inserted_records = []
#         now = datetime.now()

#         for file in files:
#             filename = secure_filename(file.filename)
#             file_bytes = file.read()
#             file.seek(0)

#             broker, json_data = process_pdf(file, category, subcategory)
#             if not json_data:
#                 continue

#             for item in json_data:
#                 # Step 1: Always create a new entity
#                 entity_info = item.get("entityTable", {})
#                 entityid = queries.create_entity(
#                     entity_info.get("scripname"),
#                     entity_info.get("scripcode"),
#                     entity_info.get("benchmark"),
#                     category,
#                     subcategory,
#                     entity_info.get("nickname"),
#                     entity_info.get("isin"),
#                     now
#                 )
#                 if not entityid:
#                     continue

#                 # Step 2: Save PDF once per entity
#                 queries.insert_pdf_file(entityid, filename, file_bytes, now)

#                 # Step 3: Handle multiple actions for this entity
#                 actions = item.get("actionTable", [])
#                 if isinstance(actions, dict):
#                     actions = [actions]

#                 for action in actions:
#                     order_number = action.get("order_number")

#                     # ✅ Prevent duplicate order_number for THIS entity only
#                     if queries.order_exists_for_entity(order_number, entityid):
#                         continue

#                     action_tuple = (
#                         action.get("scrip_code"),
#                         action.get("mode"),
#                         action.get("order_type"),
#                         action.get("scrip_name"),
#                         action.get("isin"),
#                         order_number,
#                         action.get("folio_number"),
#                         action.get("nav"),
#                         action.get("stt"),
#                         action.get("unit"),
#                         action.get("redeem_amount"),
#                         action.get("purchase_amount"),
#                         action.get("cgst"),
#                         action.get("sgst"),
#                         action.get("igst"),
#                         action.get("ugst"),
#                         action.get("stamp_duty"),
#                         action.get("cess_value"),
#                         action.get("net_amount"),
#                         now,
#                         entityid,
#                         action.get("purchase_value"),
#                         action.get("order_date"),
#                         action.get("sett_no")
#                     )
#                     queries.auto_action_table(action_tuple)

#                     inserted_records.append({
#                         "entityid": entityid,
#                         "order_number": order_number,
#                         "pdf": filename
#                     })

#         return make_response(
#             middleware.exs_msgs(inserted_records, responses.insert_200, "1020200"),
#             200
#         )

#     except Exception as e:
#         print("Error in upload_and_save:", e)
#         return make_response(
#             middleware.exe_msgs(responses.insert_501, str(e.args), "1020500"),
#             500
#         )
# old corect working code




def upload_and_save():
    try:
        if request.method != "POST":
            return make_response({"error": "Method not allowed"}, 405)

        files = request.files.getlist("files")
        if not files:
            return make_response({"error": "No files uploaded"}, 400)

        category = request.form.get("category")
        subcategory = request.form.get("subcategory")
        if not category or not subcategory:
            return make_response({"error": "Category & Subcategory required"}, 400)

        inserted_records = []
        now = datetime.now()

        for file in files:
            filename = secure_filename(file.filename)
            file_bytes = file.read()
            file.seek(0)

            broker, json_data = process_pdf(file, category, subcategory)
            if not json_data:
                continue

            for item in json_data:
                actions = item.get("actionTable", [])
                if isinstance(actions, dict):
                    actions = [actions]

                for action in actions:
                    order_number = action.get("order_number")

                    # ✅ Skip if order_number already exists globally
                    if queries.order_exists(order_number):
                        continue  

                    # ✅ Only create entity AFTER confirming order_number is new
                    entity_info = item.get("entityTable", {})
                    entityid = queries.create_entity(
                        entity_info.get("scripname"),
                        entity_info.get("scripcode"),
                        entity_info.get("benchmark"),
                        category,
                        subcategory,
                        entity_info.get("nickname"),
                        entity_info.get("isin"),
                        now
                    )
                    if not entityid:
                        continue

                    # Save PDF once per entity
                    queries.insert_pdf_file(entityid, filename, file_bytes, now)

                    # Insert action
                    action_tuple = (
                        action.get("scrip_code"),
                        action.get("mode"),
                        action.get("order_type"),
                        action.get("scrip_name"),
                        action.get("isin"),
                        order_number,
                        action.get("folio_number"),
                        action.get("nav"),
                        action.get("stt"),
                        action.get("unit"),
                        action.get("redeem_amount"),
                        action.get("purchase_amount"),
                        action.get("cgst"),
                        action.get("sgst"),
                        action.get("igst"),
                        action.get("ugst"),
                        action.get("stamp_duty"),
                        action.get("cess_value"),
                        action.get("net_amount"),
                        now,
                        entityid,
                        action.get("purchase_value"),
                        action.get("order_date"),
                        action.get("sett_no")
                    )
                    queries.auto_action_table(action_tuple)

                    inserted_records.append({
                        "entityid": entityid,
                        "order_number": order_number,
                        "pdf": filename
                    })

        return make_response(
            middleware.exs_msgs(inserted_records, responses.insert_200, "1020200"),
            200
        )

    except Exception as e:
        print("Error in upload_and_save:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), "1020500"),
            500
        )


# ============================= Auto PDF Read and Insert Into DB =========================



# /////////////////////////////TESTING////////////////////

def getDistinctEntityIds():
    try:
        return jsonify({
            "action_table": queries.get_distinct_entityids_action(),
            "direct_equity": queries.get_distinct_entityids_equity(),
            "aif_table": queries.get_distinct_entityids_aif()
        })
    except Exception as e:
        return jsonify({"error": str(e)})