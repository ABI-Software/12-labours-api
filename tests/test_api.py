import json
import requests
from app.config import Gen3Config

API_URL = 'http://localhost:8080'
GEN3_CREDENTIALS = {
    "api_key": Gen3Config.GEN3_API_KEY,
    "key_id": Gen3Config.GEN3_KEY_ID
}
TOKEN = requests.post(
    f'{Gen3Config.GEN3_ENDPOINT_URL}/user/credentials/cdis/access_token', json=GEN3_CREDENTIALS).json()
HEADER = {'Authorization': 'bearer ' + TOKEN['access_token']}


DICT_URL = f"{API_URL}/dictionary"


def test_get_dictionary():
    response = requests.get(DICT_URL)
    res = json.loads(response.content)
    assert response.status_code == 200
    assert type(response.content) is bytes
    assert len(res['dictionary']) == 28
    assert res["dictionary"] == ["aliquot", "project", "acknowledgement", "diagnosis", "clinical_test", "experimental_metadata", "demographic", "submitted_copy_number", "submitted_aligned_reads", "submitted_somatic_mutation", "slide", "keyword", "slide_count",
                                 "treatment", "read_group", "program", "core_metadata_collection", "sample", "exposure", "submitted_unaligned_reads", "experiment", "read_group_qc", "slide_image", "case", "publication", "aligned_reads_index", "family_history", "submitted_methylation"]


PROG_URL = f"{API_URL}/program"


def test_get_program():
    response = requests.get(PROG_URL)
    res = json.loads(response.content)
    assert response.status_code == 200
    assert type(response.content) is bytes
    assert len(res['program']) == 1
    assert res["program"] == ["demo1"]


PROG_NAME = "demo1"
PROJ_URL = f"{API_URL}/{PROG_NAME}/project"


def test_get_project():
    response = requests.get(PROJ_URL)
    res = json.loads(response.content)
    assert response.status_code == 200
    assert type(response.content) is bytes
    assert len(res['project']) == 2
    assert res["project"] == ["d1p1", "jenkins"]


PROJ_NAME = "jenkins"
FORM_TYPE = "json"
NODE_NAME = "sample"
NODE_URL = f"{API_URL}/nodes/{NODE_NAME}"


def test_get_project():
    payload = {
        "program": PROG_NAME,
        "project": PROJ_NAME,
        "format": FORM_TYPE,
    }
    response = requests.post(NODE_URL, payload, headers=HEADER)
    # res = json.loads(response.content)
    assert response.status_code == 200
    # assert type(response.content) is bytes
    # assert len(res['dictionary']) == 2
    # assert res["project"] == ["d1p1", "jenkins"]
