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
        


# def getentityById():
#      if request.method == 'GET':
#         try:
#             en_id = request.form['id']
#             data=queries.getentityById(en_id)
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
def update_entity_table():
    try:
        if request.method == 'PUT': 
            formData = request.get_json()

           
            formlist = (formData.get('scripname'),formData.get('scripcode'),formData.get('benchmark'),formData.get('category'),formData.get('subcategory'),formData.get('nickname'),datetime.now(),formData.get('id')
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
        if request.method == 'POST':
            formData = request.get_json()

            entityid = formData.get('entityid')
            rows = formData.get('rows')

            if not entityid or not rows or not isinstance(rows, list):
                return make_response(
                    middleware.exe_msgs(responses.insert_501, "Missing or invalid 'entityid' or 'rows' in request.", '1020501'),
                    400
                )

            inserted_ids = []
            for row in rows:
                # Prepare tuple for each row
                formlist = (row.get('company_name'),row.get('scripcode'),row.get('weightage'),row.get('sector'),row.get('isin_code'),datetime.now(),entityid)
                insert_id = queries.underlying_table(formlist)

                if type(insert_id).__name__ != "int":
                    # If any insert fails, return error immediately
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




#========================================Underlying Table End ======================================================

            

            