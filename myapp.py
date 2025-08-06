
import route
import os
# from sparkEntry import spark
# spark = spark



if __name__ == '__main__':	
	route.app.debug = True
	route.app.run(host='0.0.0.0',port=5001)
	route.app.run(debug=True)
