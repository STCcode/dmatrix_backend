from flask import Flask,session
from flask_cors import CORS,cross_origin
import psycopg2
import schedule
import time
from datetime import datetime as dt,date
import yaml
# import platform
from json_custom import init_json
import os

print(os.getcwd())

app = Flask(__name__)
CORS(app)
init_json(app)  # ✅ enable custom JSON globally
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
# app.add_url_rule('/wcare/getallrole',view_func=user_fun.getallrole,methods=['GET','POST'])
# app.add_url_rule('/wcare/saveuser',view_func=user_fun.saveuser,methods=['GET','POST'])
app.add_url_rule('/register',view_func=user_fun.save_user,methods=['GET','POST'])
app.add_url_rule('/wcare/checkusername',view_func=user_fun.checkusername,methods=['GET','POST'])
# app.add_url_rule('/wcare/getAlluser',view_func=user_fun.getAlluser,methods=['GET','POST'])
app.add_url_rule('/wcare/getAllUserById',view_func=user_fun.getAllUserById,methods=['GET','POST'])
app.add_url_rule('/wcare/updateuser',view_func=user_fun.updateuser,methods=['GET','POST'])
app.add_url_rule('/wcare/deleteuserById',view_func=user_fun.deleteuserById,methods=['GET','POST'])

#=======================================Auto Ectract and Upload ALL Actio Table Data Start====================================

# app.add_url_rule('/uploadAutomationData', view_func=user_fun.upload_and_save, methods=['POST'])
# Endpoint to upload PDFs & auto-extract
app.add_url_rule('/uploadAutomationData', view_func=user_fun.upload_and_save, methods=['POST'])


#=======================================Auto Ectract and Upload ALL Actio Table Data Start====================================


#=======================================Entity Table Start====================================

app.add_url_rule('/entity_table' ,view_func=user_fun.entity_table,methods=['GET','POST'])
app.add_url_rule('/getAllentity',view_func=user_fun.getAllentity,methods=['GET','POST'])
app.add_url_rule('/updateentity',view_func=user_fun.update_entity_table,methods=['PUT'])
app.add_url_rule('/DeleteEntityByid',view_func=user_fun.delete_entity,methods=['DELETE','POST'])

app.add_url_rule('/getEntityById',view_func=user_fun.getMutualFundDataById,methods=['GET','POST'])

app.add_url_rule('/getAllMutualFund',view_func=user_fun.getAllMutualFund,methods=['GET','POST'])


app.add_url_rule('/getCountOfAllEntity',view_func=user_fun.getCountOfAllEntity,methods=['GET','POST'])

# ======================================entity Table End======================================

#======================================= Mutual Fund Action Table Start====================================

app.add_url_rule('/action_table' ,view_func=user_fun.action_table,methods=['GET','POST'])
app.add_url_rule('/getAllAction',view_func=user_fun.getAllAction,methods=['GET','POST'])
app.add_url_rule('/getActionByentId',view_func=user_fun.getActionByentId,methods=['GET','POST'])

app.add_url_rule('/updateMFDetailActionTableRow',view_func=user_fun.updateMFDetailActionTableRow,methods=['PUT'])
app.add_url_rule('/deleteMFDetailActionTableRow',view_func=user_fun.deleteMFDetailActionTableRow,methods=['DELETE','POST'])

app.add_url_rule('/getAllActionTableOfMFEquity',view_func=user_fun.getMfByentId,methods=['GET','POST'])

app.add_url_rule('/insertMutualFundNavData',view_func=user_fun.insertMFNavData,methods=['GET','POST'])
app.add_url_rule('/getMutualFundbyIsinId',view_func=user_fun.getAllMutualFundNav,methods=['GET','POST'])

app.add_url_rule('/getAllMFEquitytotalValue',view_func=user_fun.getAllMFEquitytotalValue,methods=['GET','POST'])



# ====================================== Mutual Fund Action Table End======================================

#=======================================mcap Table Start======================================

app.add_url_rule('/mcap_table' ,view_func=user_fun.mcap_table,methods=['GET','POST'])

# ======================================mcap Table End=========================================


#========================================Underlying Table Start====================================

app.add_url_rule('/underlying_table' ,view_func=user_fun.underlying_table,methods=['GET','POST'])
app.add_url_rule('/getAllUnderlying',view_func=user_fun.getAllUnderlying,methods=['GET','POST'])
app.add_url_rule('/getUnderlyingById',view_func=user_fun.getUnderlyingById,methods=['GET','POST'])
app.add_url_rule('/getUnderlyingByMf',view_func=user_fun.getUnderlyingByMf,methods=['GET','POST'])
# app.add_url_rule('/ClearUnderlyingdata',view_func=user_fun.ClearUnderlyingdata,methods=['DELETE','POST'])
app.add_url_rule('/clearUnderlyingByEntityId',view_func=user_fun.ClearUnderlyingdata,methods=['DELETE','POST'])

# ======================================Underlying Table End========================================  


# =======================Api auto services of mutual fund NAV========================
app.add_url_rule("/start", view_func=user_fun.start_nav_scheduler, methods=["GET"])
app.add_url_rule("/stop", view_func=user_fun.stop_nav_scheduler, methods=["GET"])

# =======================end auto services fro mutual fund NAV========================


# =======================# new compare weight API Start========================

app.add_url_rule('/compare_weight', view_func=user_fun.compareEntityWeights, methods=['GET'])

# =======================# new compare weight API End========================




#========================================bigsheet Table Start====================================

app.add_url_rule('/getCamByid',view_func=user_fun.getCamByid_route,methods=['GET','POST'])

# ======================================Bigsheet Table End========================================    


#=======================================AIF Table Start====================================

app.add_url_rule('/insertAifActionData' ,view_func=user_fun.InsertAifData,methods=['GET','POST'])
app.add_url_rule('/getAllAif',view_func=user_fun.getAllAif,methods=['GET','POST'])
app.add_url_rule('/getAifActionTablebyId',view_func=user_fun.getAifActionTablebyId,methods=['GET','POST'])

app.add_url_rule('/updateAIFDetailActionTableRow',view_func=user_fun.updateAIFDetailActionTableRow,methods=['POST'])
app.add_url_rule('/deleteAIFDetailActionTableRow',view_func=user_fun.deleteAIFDetailActionTableRow,methods=['DELETE','POST'])

app.add_url_rule('/getAifEntity',view_func=user_fun.getAifEntity,methods=['GET','POST'])

app.add_url_rule('/insertNavData' ,view_func=user_fun.insertNavData,methods=['GET','POST'])
# app.add_url_rule('/getAIFEquityDetailsById',view_func=user_fun.getAIFEquityDetailsById,methods=['GET','POST'])

# ======================================AIF Table End======================================



#======================================= AIF Fix Income Table Start====================================

# app.add_url_rule('/InsertAifFixIncomeData' ,view_func=user_fun.InsertAifFixIncomeData,methods=['GET','POST'])
app.add_url_rule('/getAllAIFFixedIncomeActionTable',view_func=user_fun.getAllAifFixIncomeActionTable,methods=['GET','POST'])
app.add_url_rule('/getAIFFixedIncomeDetailsById',view_func=user_fun.getAIFFixedIncomeDetailsById,methods=['GET','POST'])

app.add_url_rule('/updateAIFFixedIncomeDetailActionTableRow',view_func=user_fun.updateAIFFixIncomeDetailActionTableRow,methods=['POST'])
app.add_url_rule('/deleteAIFFixedIncomeDetailActionTableRow',view_func=user_fun.deleteAIFFixIncomeDetailActionTableRow,methods=['DELETE','POST'])

app.add_url_rule('/getAllAIFFixedIncomeEntities',view_func=user_fun.getAllAifFixedIncomeEntity,methods=['GET','POST'])
app.add_url_rule('/getAifDetailsFixedIncomeUnderlyingTable',view_func=user_fun.getAifDetailsFixedIncomeUnderlyingTable,methods=['GET','POST'])

# app.add_url_rule('/insertNavData' ,view_func=user_fun.insertNavData,methods=['GET','POST'])
# app.add_url_rule('/getAIFFixIncomeEquityDetailsById',view_func=user_fun.getAIFFixIncomeEquityDetailsById,methods=['GET','POST'])

# ====================================== AIF Fix Income Table End======================================


#=======================================Direct Table Start============================================

app.add_url_rule('/InsertdirectData' ,view_func=user_fun.Insert_directData,methods=['GET','POST'])
app.add_url_rule('/getAllDirectEquity',view_func=user_fun.getallDirectdata,methods=['GET','POST'])
app.add_url_rule('/getdirectByentId',view_func=user_fun.getdirectByentId,methods=['GET','POST'])
app.add_url_rule('/getAllActionTableOfDirectEquity',view_func=user_fun.getAllActionTableOfDirectEquity,methods=['GET','POST'])
# app.add_url_rule('/getDirectEquityDetailsById',view_func=user_fun.getDirectEquityByid,methods=['GET','POST'])
app.add_url_rule('/getDEDetailActionTable',view_func=user_fun.getDEDetailActionTable,methods=['GET','POST'])
# ======================================Direct Table End=================================================

# ======================================Delete Etity data From Underlying,Action Table======================================
app.add_url_rule('/delete_entity_data',view_func=user_fun.delete_entity_data,methods=['DELETE','POST'])
# ======================================Delete Etity data From Underlying,Action Table======================================



#======================================= ETF etc Table Start====================================

app.add_url_rule('/insertETFActionTable',view_func=user_fun.InsertEtfData,methods=['GET','POST'])
app.add_url_rule('/getAllActionTableOfETF',view_func=user_fun.getAllEtf,methods=['GET','POST'])
app.add_url_rule('/getETFActionTablebyId',view_func=user_fun.getEtfActionTablebyId,methods=['GET','POST'])      
app.add_url_rule('/getETFEntity',view_func=user_fun.getEtfEntity,methods=['GET','POST'])
# app.add_url_rule('/getETFDetailsById',view_func=user_fun.getETFDetailsById,methods=['GET','POST'])

#==========================================Equity ETF  Table Start ====================================
app.add_url_rule('/getAllETFEquity',view_func=user_fun.getAllETFEquity,methods=['GET','POST'])
app.add_url_rule('/getAllActionTableOfETFEquity',view_func=user_fun.getAllActionTableOfETFEquity,methods=['GET','POST'])
app.add_url_rule('/getETFDetailsEquityById',view_func=user_fun.getETFDetailsEquityById,methods=['GET','POST'])
app.add_url_rule('/getETFEquityDetailActionTable',view_func=user_fun.getETFEquityDetailActionTable,methods=['GET','POST'])
app.add_url_rule('/getETFEquityDetailUnderlyingTable',view_func=user_fun.getETFEquityDetailUnderlyingTable,methods=['GET','POST'])


# ====================================== Equity ETF etc Table End =========================================

#========================================== Fix Income ETF Table Start ====================================
app.add_url_rule('/getAllFixIncomeETF',view_func=user_fun.getAllFixIncomeETF,methods=['GET','POST'])
app.add_url_rule('/getAllActionTableOfFixIncomeETF',view_func=user_fun.getAllActionTableOfFixIncomeETF,methods=['GET','POST'])
app.add_url_rule('/getFixIncomeETFById',view_func=user_fun.getFixIncomeDetailsETFById,methods=['GET','POST'])
app.add_url_rule('/getFixIncomeETFDetailActionTable',view_func=user_fun.getFixIncomeETFDetailActionTable,methods=['GET','POST'])
app.add_url_rule('/getFixIncomeEquityDetailUnderlyingTable',view_func=user_fun.getFixIncomeETFDetailUnderlyingTable,methods=['GET','POST'])



# ====================================== Fix Income Table End =========================================




#=======================================Commodities  etc Table Start====================================

app.add_url_rule('/InsertCommoditiesDirect' ,view_func=user_fun.Insert_CommoditiesDirect,methods=['GET','POST'])
app.add_url_rule('/getAllDirectEquityCommodities',view_func=user_fun.getAllDirectEquityCommodities,methods=['GET','POST'])
app.add_url_rule('/getDEDetailCommoditiesActionTable',view_func=user_fun.getCommoditiesActionTablebyId,methods=['GET','POST'])
app.add_url_rule('/getDEDetailCommoditiesEntityById',view_func=user_fun.getDEDetailCommoditiesEntityById,methods=['GET','POST'])
app.add_url_rule('/getAllActionOfDirectEquityCommodity',view_func=user_fun.getCommoditiesEntity,methods=['GET','POST'])

app.add_url_rule('/getCountOfAllCommodities',view_func=user_fun.getCountOfAllCommodities,methods=['GET','POST'])
app.add_url_rule('/getAllCommoditiesInstrument',view_func=user_fun.getAllCommoditiesInstrument,methods=['GET','POST'])

# ====================================== Commodities  etc Table End======================================



# ====================================== PMS Start=======================================================

# ==================================PMS Client Action Table Start======================================

app.add_url_rule('/insertPmsClientAction',view_func=user_fun.insertClientAction,methods=['GET','POST'])
app.add_url_rule('/getAllPmsClientActionTable',view_func=user_fun.getAllPmsClientActionTable,methods=['GET','POST'])
app.add_url_rule('/getPmsClientActionById',view_func=user_fun.getPmsClientActionById,methods=['GET','POST'])
app.add_url_rule('/getAllPMSEquity',view_func=user_fun.getPmsClientEntity,methods=['GET','POST'])
app.add_url_rule('/getPmsEquityDetailbyId',view_func=user_fun.getPmsEquityDetailbyId,methods=['GET','POST'])

#======================================= PMS AMC Action Table Start====================================

app.add_url_rule('/insertPmsAmcAction',view_func=user_fun.insertPmsAmcAction,methods=['GET','POST'])
app.add_url_rule('/getAllPmsAmcActionTable',view_func=user_fun.getAllPmsAmcActionTable,methods=['GET','POST'])
app.add_url_rule('/getPmsAmcActionById',view_func=user_fun.getPmsAmcActionById,methods=['GET','POST'])
app.add_url_rule('/getPmsAmcEntity',view_func=user_fun.getPmsAmcEntity,methods=['GET','POST'])


# ====================================== PMS End=======================================================



# ======================================Get All Equity======================================
app.add_url_rule('/getAllEquity',view_func=user_fun.getAllEquity,methods=['GET','POST'])
# ======================================Get All Equity======================================



# ======================================Get All EquityActionTable ======================================
app.add_url_rule('/getEquityActionTable',view_func=user_fun.getEquityActionTable,methods=['GET','POST'])
# ======================================Get All Equity ActionTable======================================


# ======================================== Get allMfEquityUnderlyingCount Start============================
app.add_url_rule('/getallMfEquityUnderlyingCount',view_func=user_fun.GetallMfEquityUnderlyingCount,methods=['GET','POST'])
app.add_url_rule('/getallMFEquitySectorCount',view_func=user_fun.GetallMfSectorUnderlyingCount,methods=['GET','POST'])
app.add_url_rule('/getallMFDetailsEquitySectorCount',view_func=user_fun.getallMFDetailsEquitySectorCount,methods=['GET','POST'])
app.add_url_rule('/getallMFDetailsEquityMCAPCount',view_func=user_fun.getallMFDetailsEquityMCAPCount,methods=['GET','POST'])
app.add_url_rule('/getAIFDetailsEquityMCAPCount',view_func=user_fun.getAIFDetailsEquityMCAPCount,methods=['GET','POST'])
app.add_url_rule('/getAIFDetailsEquitySectorCount',view_func=user_fun.getAIFDetailsEquitySectorCount,methods=['GET','POST'])
app.add_url_rule('/getAllAIFEquityUnderlyingCount',view_func=user_fun.GetallAIFEquityUnderlyingCount,methods=['GET','POST'])
app.add_url_rule('/getAllAIFEquitySectorCount',view_func=user_fun.GetallAIFSectorUnderlyingCount,methods=['GET','POST'])

app.add_url_rule('/getAIFDetailsFixedIncomeMCAPCount',view_func=user_fun.getAIFDetailsFixedIncomeMCAPCount,methods=['GET','POST'])
app.add_url_rule('/getallFixedIncomeUnderlyingCount',view_func=user_fun.GetallFixedIncomeUnderlyingCount,methods=['GET','POST'])
app.add_url_rule('/getAIFDetailsFixedIncomeSectorCount',view_func=user_fun.getAIFDetailsFixedIncomeSectorCount,methods=['GET','POST'])
app.add_url_rule('/getallAIFFixedIncomeSectorCount',view_func=user_fun.GetallAIFFixedIncomeSectorCount,methods=['GET','POST'])
# ======================================== Get allMfEquityUnderlyingCount End============================

# ======================================Get All Home Equity======================================
app.add_url_rule('/getAllHomeData',view_func=user_fun.getAllHomeData,methods=['GET','POST'])
# ======================================Get All Home Equity======================================

# ======================================Get All Action instrument (MutualFund)======================================
app.add_url_rule('/getAllActionInstrument',view_func=user_fun.getAllActionInstrument,methods=['GET','POST'])
# ======================================Get All Action instrument===================================================

# ======================================Get All BenchMark =====================================================
app.add_url_rule('/getBenchmarksByCategory',view_func=user_fun.getAllEntityBenchMark,methods=['GET','POST'])
# ======================================Get Get All BenchMark ===================================================




# ======================================calculate Xirr (IRR)======================================
app.add_url_rule('/getActionIRR', view_func=user_fun.getActionIRR, methods=['GET'])
app.add_url_rule('/getDirectEquityIRR', view_func=user_fun.getDirectEquityIRR, methods=['GET'])
app.add_url_rule('/getAifIRR', view_func=user_fun.getAifIRR, methods=['GET'])


app.add_url_rule('/getALLMutualFundActionTableIRR', view_func=user_fun.getALLMutualFundActionTableIRR, methods=['GET'])


app.add_url_rule('/getDirectEquityCommodityIRR', view_func=user_fun.getDirectEquityCommodityIRR, methods=['GET'])


# ======================================calculate Xirr (IRR)======================================




# //////////////////testing///////////////////////////////////////////
# Temporary debug endpoint
app.add_url_rule('/getDistinctEntityIds', view_func=user_fun.getDistinctEntityIds, methods=['GET'])
# @app.route("/getDistinctEntityIds", methods=["GET"])
# def get_distinct_entityids_route():
#     return function.getDistinctEntityIds()











