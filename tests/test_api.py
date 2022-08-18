import json
import pytest
from app import app
from app.config import Config


@pytest.fixture
def client():
    app.config["TESTING"] = True
    return app.test_client()


def test_get_dictionary(client):
    response = client.get("/dictionary")
    assert response.status_code == 200
    assert response.content_type == "application/json"
    assert b"dictionary" in response.data

    res = json.loads(response.data.decode())
    assert len(res["dictionary"]) == 31
    assert res["dictionary"] == ["root", "data_release", "aliquot", "project", "acknowledgement", "diagnosis", "clinical_test", "experimental_metadata", "demographic", "submitted_copy_number", "submitted_aligned_reads", "submitted_somatic_mutation", "slide", "keyword", "slide_count",
                                 "treatment", "read_group", "program", "core_metadata_collection", "sample", "exposure", "submitted_unaligned_reads", "experiment", "read_group_qc", "slide_image", "case", "publication", "aligned_reads_index", "family_history", "submitted_methylation", "_all"]


def test_get_program(client):
    response = client.get("/program")
    assert response.status_code == 200
    assert response.content_type == "application/json"
    assert b"program" in response.data

    res = json.loads(response.data.decode())
    assert len(res["program"]) == 1
    assert res["program"] == ["demo1"]


def test_get_project(client):
    test_data_pass = {
        "program": "demo1",
    }
    response = client.post("/projects", json=test_data_pass)
    assert response.status_code == 200
    assert b"project" in response.data

    res = json.loads(response.data.decode())
    assert len(res["project"]) == 2
    assert res["project"] == ["d1p1", "jenkins"]

    test_data_failed_400 = {}
    response = client.post("/projects", json=test_data_failed_400)
    assert response.status_code == 400

    test_data_failed_404 = {
        "program": "demo2",
    }
    response = client.post("/projects", json=test_data_failed_404)
    assert response.status_code == 404


def test_get_all_node_records(client):
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


def test_get_exact_node_record(client):
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
    response = client.post(f"/graphql", json=test_data_pass)

    assert response.status_code == 200
    assert b"data" in response.data

    test_data_failed_400 = {}
    response = client.post(f"/graphql", json=test_data_failed_400)

    assert response.status_code == 400

    test_data_failed_404 = {
        "node_type": "core_metadata_collection",
        "condition":
            '(project_id: ["demo1-jenkins"], tissue_type: ["Contrived", "Normal"])',
        "field":
            "id submitter_id sample_type tissue_type",
    }
    response = client.post(f"/graphql", json=test_data_failed_404)

    assert response.status_code == 404


def test_download_file(client):
    PROG_NAME = "demo1"
    PROJ_NAME = "jenkins"
    UUID = "1a220420-c1dd-4959-adbb-cc8a257525b2"
    FORM = "json"
    response = client.get(f"/{PROG_NAME}/{PROJ_NAME}/{UUID}/{FORM}/download")

    assert response.status_code == 200
    assert b"id" in response.data

    res = json.loads(response.data.decode())
    assert len(res) == 1
