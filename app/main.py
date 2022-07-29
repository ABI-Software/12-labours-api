# import boto3
import json
import logging
import requests
import gspread

from flask import Flask, abort, request, jsonify
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from requests.auth import HTTPBasicAuth
from oauth2client.service_account import ServiceAccountCredentials

from app.config import Config
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


@app.route("/search", methods=['GET'])
def search():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credential = ServiceAccountCredentials.from_json_keyfile_name(
        "./app/credentials.json", scope)
    client = gspread.authorize(credential)
    gsheet = client.open("test organ sheets").sheet1
    data = gsheet.get_all_records()
    return jsonify(data)


@app.route("/search/data", methods=['GET'])
def search_s3_data():
    req = requests.get(
        "https://mapcore-bucket1.s3-us-west-2.amazonaws.com/bladder/rat/rat_bladder_metadata.json")
    data = req.content
    print(req.content)
    return jsonify(data)
