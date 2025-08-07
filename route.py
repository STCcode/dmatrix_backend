from flask import Flask,session
from flask_cors import CORS,cross_origin
import psycopg2
import schedule
import time
from datetime import datetime as dt,date
import yaml
# import platform
import os

print(os.getcwd())

app = Flask(__name__)
CORS(app)
app.secret_key = 'super secret key'

db = yaml.full_load(open('db.yaml'))
# Function to get DB connection
def get_db_connection():
    return psycopg2.connect(
        host=db['postgres_host'],
        user=db['postgres_user'],
        password=db['postgres_password'],
        dbname=db['postgres_db']
    )

@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


# Example route using PostgreSQL
@app.route('/test-db')
def test_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "success", "time": str(result[0])}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == '__main__':
    app.run(debug=True)

from Execute.Functions import functions,user_fun

#----------------------------------written -----------------------

#For login page
app.add_url_rule('/', view_func=functions.index)
# app.add_url_rule('/login', view_func=functions.login,methods=['GET','POST'])

app.add_url_rule('/login', view_func=functions.login_user,methods=['GET','POST'])

app.add_url_rule('/emailhtml', view_func=functions.emailhtml)
app.add_url_rule('/landingpage', view_func=functions.landingpage)
app.add_url_rule('/main', view_func=functions.home)
app.add_url_rule('/logout', view_func=functions.logout)

#User Master 
app.add_url_rule('/wcare/getallrole',view_func=user_fun.getallrole,methods=['GET','POST'])
# app.add_url_rule('/wcare/saveuser',view_func=user_fun.saveuser,methods=['GET','POST'])
app.add_url_rule('/register',view_func=user_fun.save_user,methods=['GET','POST'])
app.add_url_rule('/wcare/checkusername',view_func=user_fun.checkusername,methods=['GET','POST'])
app.add_url_rule('/wcare/getAlluser',view_func=user_fun.getAlluser,methods=['GET','POST'])
app.add_url_rule('/wcare/getAllUserById',view_func=user_fun.getAllUserById,methods=['GET','POST'])
app.add_url_rule('/wcare/updateuser',view_func=user_fun.updateuser,methods=['GET','POST'])
app.add_url_rule('/wcare/deleteuserById',view_func=user_fun.deleteuserById,methods=['GET','POST'])

#=======================================Action Table Start====================================

app.add_url_rule('/action_table' ,view_func=user_fun.action_table,methods=['GET','POST'])

# ======================================Action Table End======================================

#=======================================Underlying Table Start====================================

app.add_url_rule('/underlying_table' ,view_func=user_fun.underlying_table,methods=['GET','POST'])

# ======================================Underlying Table End======================================  














