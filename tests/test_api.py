import json
import requests
from app.config import Gen3Config

API_URL = 'http://localhost:8080'


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


NODE_TYPE = "sample"
NODE_URL = f"{API_URL}/nodes/{NODE_TYPE}"


def test_get_all_node_records():
    payload = {
        "program": "demo1",
        "project": "jenkins",
        "format": "json",
    }
    response = requests.post(NODE_URL, data=json.dumps(payload), headers={
                             "Content-Type": "application/json"})
    res = json.loads(response.content)
    assert response.status_code == 200
    assert len(res['data']) == 10
    assert res["data"][0]["id"] == "433226d6-348f-426d-a47d-750edd59cb51"


UUID = "433226d6-348f-426d-a47d-750edd59cb51"
RECORD_URL = f"{API_URL}/records/{UUID}"


def test_get_exact_node_record():
    payload = {
        "program": "demo1",
        "project": "jenkins",
        "format": "json",
    }
    response = requests.post(RECORD_URL, data=json.dumps(payload), headers={
                             "Content-Type": "application/json"})
    res = json.loads(response.content)
    assert response.status_code == 200
    assert len(res) == 1
    assert res[0]["sample_type"] == "Primary Tumor"
    assert res[0]["preservation_method"] == "FFPE"
    assert res[0]["tissue_type"] == "Contrived"
