from crypt import methods
import json
import logging
import requests
import gspread

from flask import Flask, abort, request, jsonify
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from requests.auth import HTTPBasicAuth
from oauth2client.service_account import ServiceAccountCredentials

from app.config import Config, SpreadSheetConfig, Gen3Config
from app.dbtable import StateTable

app = Flask(__name__)
# set environment variable
app.config["ENV"] = Config.DEPLOY_ENV

CORS(app)

try:
    statetable = StateTable(Config.DATABASE_URL)
except AttributeError:
    statetable = None


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


@app.before_first_request
def start_up():
    print("Initiate")


@app.route("/")
def flask():
    return "This is the flask backend."


@app.route("/health")
def health():
    return json.dumps({"status": "healthy"})


def get_share_link(table):
    # Do not commit to database when testing
    commit = True
    if app.config["TESTING"]:
        commit = False
    if table:
        json_data = request.get_json()
        if json_data and 'state' in json_data:
            state = json_data['state']
            uuid = table.pushState(state, commit)
            return jsonify({"uuid": uuid})
        abort(400, description="State not specified")
    else:
        abort(404, description="Database not available")


def get_saved_state(table):
    if table:
        json_data = request.get_json()
        if json_data and 'uuid' in json_data:
            uuid = json_data['uuid']
            state = table.pullState(uuid)
            if state:
                return jsonify({"state": table.pullState(uuid)})
        abort(400, description="Key missing or did not find a match")
    else:
        abort(404, description="Database not available")


# An example
@app.route("/state/getshareid", methods=["POST"])
def get_share_link():
    return get_share_link(statetable)


# Get the map state using the share link id.
@app.route("/state/getstate", methods=["POST"])
def get_state():
    return get_saved_state(statetable)


SPREADSHEET_CREDENTIALS = {
    "type": SpreadSheetConfig.SHEET_TYPE,
    "project_id": SpreadSheetConfig.SHEET_PROJECT_ID,
    "private_key_id": SpreadSheetConfig.SHEET_PRIVATE_KEY_ID,
    "private_key": SpreadSheetConfig.SHEET_PRIVATE_KEY.replace('\\n', '\n'),
    "client_email": SpreadSheetConfig.SHEET_CLIENT_EMAIL,
    "client_id": SpreadSheetConfig.SHEET_CLIENT_ID,
    "auth_uri": SpreadSheetConfig.SHEET_AUTH_URI,
    "token_uri": SpreadSheetConfig.SHEET_TOKEN_URI,
    "auth_provider_x509_cert_url": SpreadSheetConfig.SHEET_AUTH_PROVIDER_X509_CERT_URL,
    "client_x509_cert_url": SpreadSheetConfig.SHEET_CLIENT_X509_CERT_URL
}


@app.route("/search", methods=['GET'])
def search():
    # Connect the backend with google spreadsheet
    scope = [
        "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"
    ]
    credential = ServiceAccountCredentials.from_json_keyfile_dict(
        SPREADSHEET_CREDENTIALS, scope)
    client = gspread.authorize(credential)
    gsheet = client.open("test organ sheets").sheet1
    data = gsheet.get_all_records()
    return jsonify(data)


GEN3_CREDENTIALS = {
    "api_key": Gen3Config.GEN3_API_KEY,
    "key_id": Gen3Config.GEN3_KEY_ID
}


@app.route('/gen3', methods=['GET', 'POST'])
def gen3():
    # Connect the backend with gen3
    token = requests.post(
        'http://gen3.abi-ctt-ctp.cloud.edu.au/user/credentials/cdis/access_token', json=GEN3_CREDENTIALS).json()

    headers = {'Authorization': 'bearer ' + token['access_token']}

    query = {'query': """{project(first:0){project_id id}}"""}
    ql = requests.post(
        'http://gen3.abi-ctt-ctp.cloud.edu.au/api/v0/submission/graphql/', json=query, headers=headers)
    return ql.text
