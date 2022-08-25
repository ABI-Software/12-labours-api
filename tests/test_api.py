import json
import pytest
from app import app
from app.config import Config


@pytest.fixture
def client():
    app.config["TESTING"] = True
    return app.test_client()


def test_get_gen3_program(client):
    response = client.get("/program")
    assert response.status_code == 200
    assert response.content_type == "application/json"
    assert b"program" in response.data

    # The program name might be changed
    # If this test failed, check Gen3 program.
    res = json.loads(response.data.decode())
    assert len(res["program"]) == 1
    assert res["program"] == ["demo1"]


def test_get_gen3_project(client):
    test_data_pass = {
        "program": "demo1",
    }
    response = client.post("/project", json=test_data_pass)
    assert response.status_code == 200
    assert b"project" in response.data

    # The project might be added or removed
    # If this test failed, check Gen3 project.
    res = json.loads(response.data.decode())
    assert len(res["project"]) == 1
    assert res["project"] == ["12L"]

    test_data_failed_400 = {}
    response = client.post("/project", json=test_data_failed_400)
    assert response.status_code == 400

    test_data_failed_404 = {
        "program": "demo2",
    }
    response = client.post("/project", json=test_data_failed_404)
    assert response.status_code == 404


def test_get_gen3_dictionary(client):
    response = client.get("/dictionary")
    assert response.status_code == 200
    assert response.content_type == "application/json"
    assert b"dictionary" in response.data

    # The dictionary might be different if program has changed
    # If the test failed, check Gen3 dictionary.
    res = json.loads(response.data.decode())
    assert len(res["dictionary"]) == 20
    assert res["dictionary"] == ["root", "data_release", "slide_count", "case", "resource", "code_description", "performance", "experiment", "program", "experimental_metadata",
                                 "core_metadata_collection", "project", "code_parameter", "slide", "dataset_description", "submission", "sample", "slide_image", "aliquot", "_all"]


def test_get_gen3_node_records(client):
    NODE_TYPE = "sample"

    test_data_pass = {
        "program": "demo1",
        "project": "jenkins",
        "format": "json",
    }
    response = client.post(f"/nodes/{NODE_TYPE}", json=test_data_pass)
    assert response.status_code == 200
    assert b"data" in response.data

    res = json.loads(response.data.decode())
    assert len(res["data"]) == 10
    assert res["data"][0]["id"] == "433226d6-348f-426d-a47d-750edd59cb51"

    test_data_failed_400 = {}
    response = client.post(f"/nodes/{NODE_TYPE}", json=test_data_failed_400)
    assert response.status_code == 400

    test_data_failed_404 = {
        "program": "demo1",
        "project": "jenkinss",
        "format": "json",
    }
    response = client.post(f"/nodes/{NODE_TYPE}", json=test_data_failed_404)
    assert response.status_code == 404


def test_get_gen3_record(client):
    UUID = "433226d6-348f-426d-a47d-750edd59cb51"

    test_data_pass = {
        "program": "demo1",
        "project": "jenkins",
        "format": "json",
    }
    response = client.post(f"/records/{UUID}", json=test_data_pass)
    assert response.status_code == 200
    assert b"id" in response.data

    res = json.loads(response.data.decode())
    assert len(res) == 1
    assert res[0]["sample_type"] == "Primary Tumor"
    assert res[0]["preservation_method"] == "FFPE"
    assert res[0]["tissue_type"] == "Contrived"


def test_graphql_filter(client):
    test_data_pass = {
        "node_type": "sample",
        "condition":
        '(project_id: ["demo1-jenkins"], tissue_type: ["Contrived", "Normal"])',
        "field":
        "id submitter_id sample_type tissue_type",
    }
    response = client.post("/graphql", json=test_data_pass)

    assert response.status_code == 200
    assert b"data" in response.data

    test_data_failed_400 = {}
    response = client.post("/graphql", json=test_data_failed_400)

    assert response.status_code == 400

    test_data_failed_404 = {
        "node_type": "core_metadata_collection",
        "condition":
            '(project_id: ["demo1-jenkins"], tissue_type: ["Contrived", "Normal"])',
        "field":
            "id submitter_id sample_type tissue_type",
    }
    response = client.post("/graphql", json=test_data_failed_404)

    assert response.status_code == 404


def test_download_gen3_metadata_file(client):
    PROG_NAME = "demo1"
    PROJ_NAME = "jenkins"
    UUID = "1a220420-c1dd-4959-adbb-cc8a257525b2"
    FORM = "json"
    NAME = "testname"
    response = client.get(
        f"/download/metadata/{PROG_NAME}/{PROJ_NAME}/{UUID}/{FORM}/{NAME}")

    assert response.status_code == 200
    assert b"id" in response.data

    res = json.loads(response.data.decode())
    assert len(res) == 1


def test_get_irods_collections(client):
    # GET
    response = client.get("/irods")

    assert response.status_code == 200
    assert response.content_type == "application/json"
    assert b"folders" in response.data
    assert b"files" in response.data

    # POST
    test_post_data_pass = {
        "path": "/tempZone/home/rods/datasets"
    }
    response = client.post("/irods", json=test_post_data_pass)

    assert response.status_code == 200

    test_post_data_failed_1 = {}
    response = client.post("/irods", json=test_post_data_failed_1)

    assert response.status_code == 404

    test_post_data_failed_2 = {
        "path": "/tempZone/home/rods/data"
    }
    response = client.post("/irods", json=test_post_data_failed_2)

    assert response.status_code == 404


def test_download_irods_data_file(client):
    SUFFIX = "datasets&dataset-217-version-2&primary&pig_brainstem_provenance.json"
    response = client.get(f"/download/data/{SUFFIX}")

    assert response.status_code == 200
