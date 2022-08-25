import json
import requests
import gspread
import mimetypes
import pandas as pd
import os

from app.config import Config, S3Config, SpreadSheetConfig, Gen3Config, S3Config, iRODSConfig
from app.dbtable import StateTable

from flask import Flask, abort, request, jsonify, Response
from flask_cors import CORS
from oauth2client.service_account import ServiceAccountCredentials

from irods.session import iRODSSession

app = Flask(__name__)
# set environment variable
app.config["ENV"] = Config.DEPLOY_ENV

CORS(app)

BAD_REQUEST = 400
NOT_FOUND = 404

SPREADSHEET_CREDENTIALS = {
    "type": SpreadSheetConfig.SHEET_TYPE,
    "project_id": SpreadSheetConfig.SHEET_PROJECT_ID,
    "private_key_id": SpreadSheetConfig.SHEET_PRIVATE_KEY_ID,
    "private_key": SpreadSheetConfig.SHEET_PRIVATE_KEY.replace("\\n", "\n"),
    "client_email": SpreadSheetConfig.SHEET_CLIENT_EMAIL,
    "client_id": SpreadSheetConfig.SHEET_CLIENT_ID,
    "auth_uri": SpreadSheetConfig.SHEET_AUTH_URI,
    "token_uri": SpreadSheetConfig.SHEET_TOKEN_URI,
    "auth_provider_x509_cert_url": SpreadSheetConfig.SHEET_AUTH_PROVIDER_X509_CERT_URL,
    "client_x509_cert_url": SpreadSheetConfig.SHEET_CLIENT_X509_CERT_URL
}

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
    return "This is the 12 Labours Portal backend."


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
        if json_data and "state" in json_data:
            state = json_data["state"]
            uuid = table.pushState(state, commit)
            return jsonify({"uuid": uuid})
        abort(400, description="State not specified")
    else:
        abort(404, description="Database not available")


def get_saved_state(table):
    if table:
        json_data = request.get_json()
        if json_data and "uuid" in json_data:
            uuid = json_data["uuid"]
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
def spreadsheet():
    """
    Return the spreadsheet data.
    """
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


@app.route("/s3", methods=["POST"])
def s3():
    """
    Return the s3 data from s3 url location.
    """
    post_data = request.get_json()
    suffix = post_data.get("suffix")
    if suffix == None:
        abort(BAD_REQUEST)

    try:
        res = requests.get(f"{S3Config.S3_ENDPOINT_URL}/{suffix}")

        return res.content
    except Exception as e:
        abort(NOT_FOUND, description=str(e))


@app.route("/download/s3data/<suffix>", methods=["GET"])
def download_s3_data(suffix):
    """
    Return a single download file for a given suffix.

    :param suffix: Part of the s3 location.
    :return: The file content.
    """
    url_suffix = suffix.replace("&", "/")
    try:
        res = requests.get(f"{S3Config.S3_ENDPOINT_URL}/{url_suffix}")

        return Response(res.content,
                        mimetype="application/json",
                        headers={"Content-Disposition":
                                 f"attachment;filename={suffix}.json"})
    except Exception as e:
        abort(NOT_FOUND, description=str(e))


#
# Gen3 Data Commons
#

def get_gen3_header():
    GEN3_CREDENTIALS = {
        "api_key": Gen3Config.GEN3_API_KEY,
        "key_id": Gen3Config.GEN3_KEY_ID
    }
    try:
        TOKEN = requests.post(
            f"{Gen3Config.GEN3_ENDPOINT_URL}/user/credentials/cdis/access_token", json=GEN3_CREDENTIALS).json()
        return {"Authorization": "bearer " + TOKEN["access_token"]}
    except Exception as e:
        abort(NOT_FOUND, description=str(e))


@app.route("/program", methods=["GET"])
def get_gen3_program():
    """
    Return the program information from Gen3 Data Commons
    """
    header = get_gen3_header()
    try:
        res = requests.get(
            f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/", headers=header)

        json_data = json.loads(res.content)
        program_list = []
        for ele in json_data["links"]:
            program_list.append(ele.replace(
                "/v0/submission/", ""))
        new_json_data = {"program": program_list}
        return new_json_data
    except Exception as e:
        abort(NOT_FOUND, description=str(e))


@app.route("/project", methods=["POST"])
def get_gen3_project():
    """
    Return project information in the Gen3 program
    """
    header = get_gen3_header()
    post_data = request.get_json()
    program = post_data.get("program")
    if program == None:
        abort(BAD_REQUEST)

    try:
        res = requests.get(
            f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}", headers=header)

        json_data = json.loads(res.content)
        project_list = []
        for ele in json_data["links"]:
            project_list.append(ele.replace(
                f"/v0/submission/{program}/", ""))
        new_json_data = {"project": project_list}
        return new_json_data
    except Exception as e:
        abort(NOT_FOUND, description=str(e))


@app.route("/dictionary", methods=["GET"])
def get_gen3_dictionary():
    """
    Return all dictionary node from Gen3 Data Commons
    """
    header = get_gen3_header()
    try:
        res = requests.get(
            f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/_dictionary", headers=header)

        json_data = json.loads(res.content)
        dictionary_list = []
        for ele in json_data["links"]:
            dictionary_list.append(ele.replace(
                "/v0/submission/_dictionary/", ""))
        new_json_data = {"dictionary": dictionary_list}
        return new_json_data
    except Exception as e:
        abort(NOT_FOUND, description=str(e))


def is_json(json_data):
    """
    Returns true if the given string is a valid json.

    :param json_data: The input data need to be checked.
    :return: True if the string can be parsed as valid json.
    """
    try:
        json.loads(json_data)
    except ValueError:
        return False
    return True


@app.route("/nodes/<node_type>", methods=["POST"])
def get_gen3_node_records(node_type):
    """
    Return all records in a dictionary node.

    :param node_type: The dictionary node to export.
    :return: A list of json object containing all records in the dictionary node.
    """
    header = get_gen3_header()
    post_data = request.get_json()
    program = post_data.get("program")
    project = post_data.get("project")
    format = post_data.get("format")
    if program == None or project == None or format == None:
        abort(BAD_REQUEST)

    res = requests.get(
        f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}/{project}/export/?node_label={node_type}&format={format}", headers=header)

    json_data = json.loads(res.content)
    if is_json(res.content) and "data" in json_data and json_data["data"] != []:
        return res.content
    else:
        abort(NOT_FOUND)


@app.route("/records/<uuids>", methods=["POST"])
def get_gen3_record(uuids):
    """
    Return the fields of one or more records in a dictionary node.

    :param uuids: uuids of the records (use comma to separate the uuids e.g. uuid1,uuid2,uuid3).
    :return: A list of json object
    """
    header = get_gen3_header()
    post_data = request.get_json()
    program = post_data.get("program")
    project = post_data.get("project")
    format = post_data.get("format")
    if program == None or project == None or format == None:
        abort(BAD_REQUEST)

    res = requests.get(
        f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}/{project}/export/?ids={uuids}&format={format}", headers=header)

    json_data = json.loads(res.content)
    if b"id" in res.content:
        return res.content
    else:
        abort(NOT_FOUND, description=json_data["message"])


@app.route("/graphql", methods=["POST"])
# Only used for filtering the files in a specific node for now
def graphql_filter():
    """
    Return filtered metadata records. The query uses GraphQL query.
    """
    header = get_gen3_header()
    post_data = request.get_json()
    node_type = post_data.get("node_type")
    # Condition post format should looks like
    # '(project_id: ["demo1-jenkins", ...], tissue_type: ["Contrived", "Normal", ...], ...)'
    condition = post_data.get("condition")
    # Field post format should looks like
    # "submitter_id tissue_type tumor_code ..."
    field = post_data.get("field")
    if node_type == None or condition == None or field == None:
        abort(BAD_REQUEST)

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
        f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/graphql/", json=query, headers=header)

    json_data = json.loads(res.content)
    if json_data["data"] == None:
        abort(NOT_FOUND, description=json_data["errors"])
    else:
        if json_data["data"][f"{node_type}"] == []:
            abort(NOT_FOUND)
        else:
            return res.content


@app.route("/download/metadata/<program>/<project>/<uuid>/<format>/<filename>", methods=["GET"])
def download_gen3_metadata_file(program, project, uuid, format, filename):
    """
    Return a single file for a given uuid.

    :param program: program name.
    :param project: project name.
    :param uuid: uuid of the file.
    :param format: format of the file (must be one of the following: json, tsv).
    :param filename: name of the file.
    :return: A JSON or CSV file containing the metadata.
    """
    header = get_gen3_header()
    try:
        res = requests.get(
            f"{Gen3Config.GEN3_ENDPOINT_URL}/api/v0/submission/{program}/{project}/export/?ids={uuid}&format={format}", headers=header)

        if format == "json":
            return Response(res.content,
                            mimetype="application/json",
                            headers={"Content-Disposition":
                                     f"attachment;filename={filename}.json"})
        else:
            return Response(res.content,
                            mimetype="text/csv",
                            headers={"Content-Disposition":
                                     f"attachment;filename={filename}.csv"})
    except Exception as e:
        abort(NOT_FOUND, description=str(e))

#
# iRODS
#


def get_irods_session():
    """
    This function is used to connect to the iRODS server, it requires "host", "port", "user", "password" and "zone" environment variables.
    """
    session = iRODSSession(host=iRODSConfig.IRODS_HOST,
                           port=iRODSConfig.IRODS_PORT,
                           user=iRODSConfig.IRODS_USER,
                           password=iRODSConfig.IRODS_PASSWORD,
                           zone=iRODSConfig.IRODS_ZONE)
    return session


def get_data_list(collect):
    collect_list = []
    for ele in collect:
        collect_list.append({
            "id": ele.id,
            "name": ele.name,
            "path": ele.path
        })
    return collect_list


@app.route("/irods", methods=["GET", "POST"])
def get_irods_collections():
    """
    Return collections from a folder.

    "GET" method will return all collections from the root folder
    "POST" method will return all collections from the required folder
    """
    session = get_irods_session()
    try:
        if request.method == "GET":
            collect = session.collections.get(
                f"{iRODSConfig.IRODS_ENDPOINT_URL}")
        else:
            post_data = request.get_json()
            path = post_data.get("path")
            if path == None:
                abort(BAD_REQUEST)

            collect = session.collections.get(path)
    except Exception as e:
        abort(NOT_FOUND, description=str(e))

    folders = get_data_list(collect.subcollections)
    files = get_data_list(collect.data_objects)
    return {"folders": folders, "files": files}


@app.route("/download/data/<suffix>", methods=["GET"])
def download_irods_data_file(suffix):
    """
    Return a specific download file from iRODS.

    :param suffix: Required iRODS file path.
    :return: A file with data.
    """
    session = get_irods_session()
    url_suffix = suffix.replace("&", "/")
    try:
        file = session.data_objects.get(
            f"{iRODSConfig.IRODS_ENDPOINT_URL}/{url_suffix}")

        with file.open("r") as f:
            if suffix.endswith(".xlsx"):
                download_path = os.path.join(
                    os.path.expanduser('~'), "Downloads")
                pd.read_excel(f).to_excel(f"{download_path}/{file.name}")
                # content = pd.read_excel(f).to_csv()
                return "Successfully download!"
            else:
                if suffix.endswith(".csv"):
                    content = pd.read_csv(f).to_csv()
                else:
                    content = f.read()
            return Response(content,
                            mimetype=mimetypes.guess_type(file.name)[0],
                            headers={"Content-Disposition":
                                     f"attachment;filename={file.name}"})
    except Exception as e:
        abort(NOT_FOUND, description=str(e))
