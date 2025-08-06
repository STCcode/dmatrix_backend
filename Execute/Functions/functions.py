from flask import session, redirect, url_for, request, render_template,flash,jsonify,make_response
#from route import app
from Execute import queries,responses,middleware

def index():
    return render_template('index.html')

def emailhtml():
    return render_template('emailHtml.html')

# def login():
#     try:
#         if request.method == 'POST':
#             #print('login post')
#             # print('username_form')
#             data = request.get_json()
#             name = data['username']
#             password = data['password']
            
#             fromdata=(name, password)
#             data = queries.login(fromdata)
#             if type(data).__name__  != "list":
#                 if data.json:
#                         result=data
#                         status=500
#             else:
#                 if len(data) > 0:
#                     session['username'] = name
                
#                 result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
#                 status=200
                
#             return make_response(result,status)
          
            
#     except Exception as e:
#                 print("Error in login Data========================", e)
#                 return responses.getAll_501

############# postgres querty#######################3

def login_user():
    try:
        if request.method == 'POST':
            data = request.get_json()

            # Input validation
            if 'username' not in data or 'password' not in data:
                return make_response(
                    middleware.exe_msgs(responses.getAll_501, "Missing username or password", '1023201'),
                    400
                )

            username = data['username']
            password = data['password']
            formdata = (username, password)

            # Fetch user from DB
            user_data = queries.login_user(formdata)

            # Check response type
            if type(user_data).__name__ != "list":
                if hasattr(user_data, 'json'):
                    result = user_data
                    status = 500
            else:
                if len(user_data) > 0:
                    session['username'] = username  # Set session
                    result = middleware.exs_msgs(user_data, responses.getAll_200, '1023200')
                    status = 200
                else:
                    result = middleware.exe_msgs(responses.getAll_501, "Invalid credentials", '1023202')
                    status = 401

            return make_response(result, status)

    except Exception as e:
        print("Error in login Data ========================", e)
        return make_response(
            middleware.exe_msgs(responses.getAll_501, str(e.args), '1023203'),
            500
        )
    
# end


def home():
    if 'username' in session:
        return render_template('main.html')
    else:
        return redirect(url_for('index'))


def landingpage():
    if 'username' in session:
        return render_template('landpage.html')
    else:
        return redirect(url_for('index'))


def logout():
	session.pop('username', None)
	flash('You were logged out')
	return redirect(url_for('index'))

def getAllUserData():
    try:
        if request.method == 'POST':
            data= queries.getAllUserData()
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
            
            return make_response(result,status)
    except Exception as e:
                print("Error in getting All userdata data==================================",e)
                return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)

def getAllFunctions():
    try:
        if request.method == 'POST':
            data= queries.getAllFunctions()
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
            
            return make_response(result,status)
    except Exception as e:
                print("Error in getting All functions data==================================",e)
                return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)
def getCustomerFilter():
    try:
        if request.method == 'POST':
            data= queries.getCustomerFilter()
            if type(data).__name__  != "list":
                if data.json:
                    result=data
                    status=500
            else:
                result=middleware.exs_msgs(data,responses.getAll_200,'1023200')
                status=200
            
            return make_response(result,status)
    except Exception as e:
                print("Error in getting All getCustomerFilter data==================================",e)
                return  make_response(middleware.exe_msgs(responses.getAll_501,str(e.args),'1023500'),500)