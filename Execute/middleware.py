from Execute import responses
from flask import make_response,jsonify

def exe_msgs(errmsg,msg,code):
        return jsonify({"errmsgs":msg,"error":errmsg,"code":code})

def query_msgs(msg):
        if msg == responses.execution_501:
                #return responses.queryError_501
                make_response(responses.queryError_501, 500)
        else:
                return msg

def exs_msgs(data,msg,code):
        return jsonify({"data":data,"successmsgs":msg,"code":code}) 


# def exs_msgs(result, success_msg, code):
#     if isinstance(result, dict) and "error" in result:
#         return {
#             "success": False,
#             "message": result,
#             "code": "1020500"
#         }
#     return {
#         "success": True,
#         "message": success_msg,
#         "response": result,
#         "code": code
#     }


''' def insert_msgs(msg):
    return jsonify({"successmsgs":responses.insert_200})

def update_msgs(msg):
    if msg == None:
            return jsonify({"errmsgs":responses.update_501})
    else:
            return jsonify({"successmsgs":responses.update_200})

def getDatabyId_msgs(msg):
    if msg == None:
            return jsonify({"errmsgs":responses.getById_501})
    else:
            return jsonify({"successmsgs":responses.getById_200,"data":msg})

def delete_msgs(msg):
    if msg == None:
        return jsonify({"errmsgs":responses.delete_501})
    else:
        return jsonify({"successmsgs":responses.delete_200})

def insert_return_id(msg):
    if msg == None:
            return responses.insert_501
    else:
            return msg
 '''