import json
import requests
import gspread

from flask import Flask, abort, request, jsonify
from flask_cors import CORS
from oauth2client.service_account import ServiceAccountCredentials

from app.config import Config, SpreadSheetConfig, Gen3Config
from app.dbtable import StateTable

app = Flask(__name__)
# set environment variable
app.config["ENV"] = Config.DEPLOY_ENV

CORS(app)

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

GEN3_CREDENTIALS = {
    "api_key": Gen3Config.GEN3_API_KEY,
    "key_id": Gen3Config.GEN3_KEY_ID
}

TOKEN = requests.post(
    f'{Gen3Config.GEN3_ENDPOINT_URL}/user/credentials/cdis/access_token', json=GEN3_CREDENTIALS).json()
HEADER = {'Authorization': 'bearer ' + TOKEN['access_token']}

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


@app.route("/spreadsheet")
# Connect to the google spreadsheet and get all spreadsheet data.
def spreadsheet():
    scope = [
        "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"
    ]
    credential = ServiceAccountCredentials.from_json_keyfile_dict(
        SPREADSHEET_CREDENTIALS, scope)
    client = gspread.authorize(credential)
    gsheet = client.open("organ_sheets").sheet1
    data = gsheet.get_all_records()
    return jsonify(data)


@app.route("/program", methods=['GET', 'POST'])
# Get the program information from Gen3 Data Commons
def program():
    res = requests.get(
        f'{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/', headers=HEADER)

    json_data = json.loads(res.content)
    program_list = []
    for ele in json_data['links']:
        program_list.append(ele.replace(
            "/v0/submission/", ""))
    new_json_data = {'program': program_list}
    return new_json_data


@app.route("/<program>/project", methods=['GET'])
# Get all projects information from Gen3 Data Commons
def project(program):
    res = requests.get(
        f'{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}', headers=HEADER)

    json_data = json.loads(res.content)
    project_list = []
    for ele in json_data['links']:
        project_list.append(ele.replace(
            f"/v0/submission/{program}/", ""))
    new_json_data = {'project': project_list}
    return new_json_data


@app.route("/dictionary", methods=['GET', 'POST'])
# Get all dictionary node from Gen3 Data Commons
def dictionary():
    res = requests.get(
        f'{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/_dictionary', headers=HEADER)

    json_data = json.loads(res.content)
    dictionary_list = []
    for ele in json_data['links'][2:30]:
        dictionary_list.append(ele.replace(
            "/v0/submission/_dictionary/", ""))
        # dictionary_list.append(ele.replace(
        #     "/v0/submission/_dictionary/", "").replace("_", " ").title())
    new_json_data = {"dictionary": dictionary_list}
    return new_json_data


@app.route('/nodes/<node_type>', methods=['GET', 'POST'])
# Exports all records in a dictionary node
def export_node(node_type):
    post_data = request.get_json()
    program = post_data.get('program')
    project = post_data.get('project')
    format = post_data.get('format')
    res = requests.get(
        f'{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}/{project}/export/?node_label={node_type}&format={format}', headers=HEADER)
    return res.content



@app.route('/records/<uuid>', methods=['GET', 'POST'])
# Exports one or more records, use comma to separate the uuids
# e.g. uuid1,uuid2,uuid3
def export_record(uuid):
    post_data = request.get_json()
    program = post_data.get('program')
    project = post_data.get('project')
    format = post_data.get('format')
    res = requests.get(
        f'{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}/{project}/export/?ids={uuid}&format={format}', headers=HEADER)
    return res.content


@app.route('/graphql', methods=['GET', 'POST'])
# Only used for filtering the files in a specific node for now
def graphql_filter():
    post_data = request.get_json()
    node_type = post_data.get('node_type')
    # Condition post format should looks like
    # 'project_id: ["demo1-jenkins", ...], tissue_type: ["Contrived", "Normal", ...], ...'
    condition = post_data.get('condition')
    # Field post format should looks like
    # "submitter_id tissue_type tumor_code ..."
    field = post_data.get('field')
    query = {
        "query":
        """{""" +
        f"""{node_type}{condition}""" +
        """{""" +
        f"""{field}""" +
        """}""" +
        """}"""
    }
    res = requests.post(
        f'{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/graphql/', json=query, headers=HEADER)
    return res.content
