import re
import os
import jwt 
import json
import logging
import datetime
import requests
import configparser
from os.path import join
from flask_cors import CORS
from flask import request, jsonify
# from flask_talisman import Talisman
from flask import Flask, Response, abort
from flask.logging import default_handler
from werkzeug.utils import secure_filename
from werkzeug.exceptions import HTTPException
# from flask_wtf.csrf import CSRFProtect, generate_csrf

import db
import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

app = Flask(__name__, instance_relative_config=True)

config = configparser.ConfigParser()
config.read('config_file.ini')

SECRET_KEY = config.get("flask_key", "SECRET_KEY")
SERVER_NAME = config.get("csrf_server_name", "server_name")

app.config['SECRET_KEY'] = SECRET_KEY
# app.config['SERVER_NAME'] = SERVER_NAME

CORS(app, resources={r"/*": {"origins": "*", "send_wildcard": "False"}})


# UPLOAD_FOLDER = join(os.getcwd(),'uploads/')
UPLOAD_FOLDER = join('/mnt/prod-welcome-call/','uploads/')

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['UPLOAD_EXTENSIONS'] = ['.mp4']


#### logging config ####
app.logger.removeHandler(default_handler)
log_path = join(os.getcwd(), 'logs/app.log')
logging.basicConfig(filename=log_path, level=logging.ERROR,format='%(asctime)s: %(message)s')

def check_token(func):
	def decorated(*args, **kwargs):

		if 'Authorization' in request.headers and request.headers['Authorization'].startswith('Bearer '):
			token = request.headers['Authorization'].split(None,1)[1].strip()
		if not token:
			msg = security_check(jsonify('missing token'))
			return msg, 401
		try: 
			data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256', ])
		except:
			msg = security_check(jsonify('invalid token'))
			return msg, 401
		return func(data['user'],*args, **kwargs)
	return decorated

#### error handlers ####
# http exceptions handler
@app.errorhandler(HTTPException)
def handle_exception(e):
	"""Return JSON instead of HTML for HTTP errors."""
	response = e.get_response()
	response.data = json.dumps({
		"code": e.code,
		"name": e.name,
		"description": e.description,
		"url": request.url,
	})
	response.content_type = "application/json"
	logging.error('########### ' + str(e) + ' ###########', exc_info=True)
	return response, 500

@app.errorhandler(Exception)
def handle_exception(e):
	# pass through HTTP errors
	if isinstance(e, HTTPException):
		return e, 500
	err = {
		"code": -1,
		"name": "Server Error",
		"description": "Unexpected Error. Please contact Admin"
	}
	logging.error('########### ' + str(e) + ' ###########', exc_info=True)
	return jsonify(err), 500



def security_check(input):
	response = jsonify(input)
	response.headers['X-XSS-Protection'] = '1; mode=block'
	response.headers['Content-Security-Policy'] = "default-src 'none'"
	response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
	return response

def check_is_alpha_num(text):
	if re.match("^[A-Z0-9]+$",text):
		return True
	return False


def get_customer_care_token():
    url = config.get('customer', 'auth_url')
    username = config.get('customer', 'username')
    password = config.get('customer', 'password')
    app_id = config.get('customer', 'app_id')
    app_key = config.get('customer', 'app_key')
	
    payload = json.dumps({
    "username": username,
    "password": password
    })
    headers = {
    'app_id': app_id,
    'app_key': app_key,
    'Content-Type': 'application/json'
    }
    response = requests.request("POST",url,headers=headers,data=payload)
    token = response.json()
    return token 

def push_data(policy_no):
    token = get_customer_care_token()
    url = config.get('customer', 'push_url')
    app_id = config.get('customer', 'app_id')
    app_key = config.get('customer', 'app_key')
	
    auth_token = "Bearer " + token['authToken']
    name_no = db.username_phone(policy_no)
    name_no['Mobile_Number'] = str(name_no['Mobile_Number']) # API needs string and we had int
    payload = json.dumps([{
    "customerName": name_no['First_Name'],
    "contactNumber": name_no['Mobile_Number'],
    "policyNumber": policy_no,
     "language": ""
     }])
    headers = {
    'app_id': app_id,
    'app_key': app_key,
    'Authorization': auth_token,
    'Content-Type': 'application/json'
	}
    response = requests.request("POST", url, headers=headers, data=payload)
    customer_care_response = str(response.json())
    if response.status_code == 200:
        db.customer_care_insert(Customer_Name=name_no['First_Name'], Mobile_Number=name_no['Mobile_Number'], Policy_Number=policy_no, Language="",Customer_Care_Response = customer_care_response)

@app.route('/upload_video', methods=['POST'], endpoint='upload_file')
@check_token
def upload_file(policy):
	if request.method == 'POST':
		uploaded_file = request.files['file']
		filename = secure_filename(uploaded_file.filename)
		#requested_path = os.path.abspath(filename)

		if filename != '':
			file_ext = os.path.splitext(filename)[1]
			
			if file_ext not in app.config['UPLOAD_EXTENSIONS']:
				abort(400)

			saved_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
			uploaded_file.save(saved_file_path)
			data = [{"Saved_Path": "Uploaded"}]
			print(data)
		
		db.user_journey_flag(policy)
		

		output_sanitized = security_check(data)

		return output_sanitized, 200
	else:
		msg = security_check("The server is working")
		return msg, 200


@app.route('/user_info', methods=['POST'], endpoint='user_info')
@check_token
def user_info(policy):
	if request.method == 'POST':

		policy_no = policy
		request_data = request.get_json()
		
		if not check_is_alpha_num(policy_no):
			error = "Error:{} is not alphanumeric. Allowed characters: A-Z,0-9".format(policy_no)
			error = security_check(error)
			return error, 404

		user_details = db.user_info(policy_no)
		user_details_sanitized = security_check(user_details)

		return user_details_sanitized, 200

	else:
		message = security_check("Please use POST request")
		return message, 404


@app.route('/policy_info', methods=['POST'], endpoint='policy_info')
@check_token
def policy_info(policy):

	if request.method == 'POST':

		policy_no = policy
		request_data = request.get_json()
		
		if not check_is_alpha_num(policy_no):
			error = "Error:{} is not alphanumeric. Allowed characters: A-Z,0-9".format(policy_no)
			error = security_check(error)
			return error, 404

		policy_details = db.policy_info(policy_no)
		policy_details_sanitized = security_check(policy_details)
		
		return policy_details_sanitized, 200

	else:
		message = security_check("Please use POST request")
		return message, 404

@app.route('/insured_details', methods=['POST'], endpoint='insured_details')
@check_token
def insured_details(policy):

	if request.method == 'POST':

		policy_no = policy
		request_data = request.get_json()
		
		if not check_is_alpha_num(policy_no):
			error = "Error:{} is not alphanumeric. Allowed characters: A-Z,0-9".format(policy_no)
			error = security_check(error)
			return error, 404

		insured_details = db.insured_info(policy_no)
		insured_details_sanitized = security_check(insured_details)
		
		return insured_details_sanitized, 200

	else:
		message = security_check("Please use POST request")
		return message, 404

@app.route('/ported_policy_info', methods=['POST'], endpoint='ported_policy_info')
@check_token
def ported_policy_info(policy):

	if request.method == 'POST':

		policy_no = policy

		request_data = request.get_json()
		
		if not check_is_alpha_num(policy_no):
			error = "Error:{} is not alphanumeric. Allowed characters: A-Z,0-9".format(policy_no)
			error = security_check(error)
			return error, 404

		ported_policy_details = db.ported_policy_info(policy_no)

		if type(ported_policy_details) == str:
			return ported_policy_details, 404

		ported_policy_sanitized = security_check(ported_policy_details)
		
		return ported_policy_sanitized, 200

	else:
		message = security_check("Please use POST request")
		return message, 404

@app.route('/user_consent', methods=['POST'], endpoint='user_consent')
@check_token
def user_consent(policy):
    policy_no = policy
    request_data = request.get_json()
		
    if not check_is_alpha_num(policy_no):
        error = "Error:{} is not alphanumeric. Allowed characters: A-Z,0-9".format(policy_no)
        error = security_check(error)
        return error, 404

    page_name = request_data['page_name']
    consent = request_data['consent']
	
    consent_flag = 0
    success = False

    if consent == "Yes":
        consent_flag = 1
        success = True
    else:
        push_data(policy_no)

    db.user_disagree_db(policy_no, page_name, consent_flag)

    #ret = json.dumps(success)
    return security_check(success)


@app.route('/feedback', methods=['POST'], endpoint='feedback')
@check_token
def feedback(policy):

	if request.method == 'POST':

		policy_no = policy

		request_data = request.get_json()
		
		if not check_is_alpha_num(policy_no):
			error = "Error:{} is not alphanumeric. Allowed characters: A-Z,0-9".format(policy_no)
			error = security_check(error)
			return error, 404
		
		feedback = request_data['feedback']
		db_return = db.feedback(policy_no, feedback)
		
		db_return = security_check(db_return)
		return db_return, 200
	else:
		message = security_check("Please use POST request")
		return message, 404


def validate(date_text):
	try:
		if date_text != datetime.datetime.strptime(date_text, "%d-%m-%Y").strftime('%d-%m-%Y'):
			raise ValueError
		return True
	except ValueError:
		return False


@app.route('/login', methods=['POST'])
def login():
	if request.method == 'POST':
		
		request_data = request.get_json()
		uid = request_data['policy_no']
		dob = request_data['dob']

		final, flag = db.login_check(uid)

		if validate(dob) == False:
			message = security_check("Incorrect data format, should be DD-MM-YYYY")
			return message, 404

		if final == None:
			message = security_check("Input id is not found")
			return message, 404

		elif final[0] == datetime.datetime.strptime(dob, "%d-%m-%Y").date():
			payload = {'user' : final[1], 
						'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=30)}

			token = jwt.encode(payload, app.config['SECRET_KEY'])
			data = [{"token": token, "journey_flag": flag}]

			res = jsonify({'data': data})
			res.headers['X-XSS-Protection'] = '1; mode=block'
			res.headers['Content-Security-Policy'] = "default-src 'none'"
			res.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
			return res, 200

		else:
			message = security_check('Input DOB mismatching')
			return message, 404 
	else:
		message = security_check("Please use POST request")
		return message, 404


if __name__ == '__main__':
	app.run(port=5000, debug=False)
