import contextlib
import datetime
import json
from pathlib import Path
import subprocess
from unittest import mock

import pytest

import openeo_driver.testing
from openeo_driver.testing import TEST_USER_AUTH_HEADER, TEST_USER, read_file, load_json, TIFF_DUMMY_DATA
from openeo_driver.views import app
from openeogeotrellis.backend import GpsBatchJobs
import openeogeotrellis.job_registry
from openeogeotrellis.testing import KazooClientMock


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version(request):
    return request.param


@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeo.net'
    return app.test_client()


class ApiTester(openeo_driver.testing.ApiTester):
    data_root = Path(__file__).parent / "data"


@pytest.fixture
def api(api_version, client) -> ApiTester:
    return ApiTester(api_version=api_version, client=client)


@pytest.fixture
def api100(client) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=client)


def test_file_formats(api100):
    formats = api100.get('/file_formats').assert_status_code(200).json
    assert "GeoJSON" in formats["input"]
    assert "GTiff" in formats["output"]
    assert "CovJSON" in formats["output"]
    assert "NetCDF" in formats["output"]


def test_health(api):
    resp = api.get('/health').assert_status_code(200)
    assert resp.json == {"health": 'Health check: 14'}


class TestBatchJobs:
    DUMMY_PROCESS_GRAPH = {
        "foo": {
            "process_id": "foo",
            "arguments": {}
        }
    }

    @staticmethod
    @contextlib.contextmanager
    def _mock_kazoo_client():
        zk_client = KazooClientMock()
        with mock.patch.object(openeogeotrellis.job_registry, 'KazooClient', return_value=zk_client):
            yield zk_client

    @staticmethod
    @contextlib.contextmanager
    def _mock_utcnow():
        with mock.patch('openeogeotrellis.job_registry.datetime', new=mock.Mock(wraps=datetime.datetime)) as dt:
            dt.utcnow.return_value = datetime.datetime(2020, 4, 20, 16, 4, 3)
            yield dt.utcnow

    def test_get_user_jobs_no_auth(self, api):
        api.get('/jobs').assert_status_code(401).assert_error_code("AuthenticationRequired")

    def test_get_user_jobs_empty(self, api):
        with self._mock_kazoo_client() as zk:
            result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert result == {"jobs": [], "links": []}

    def test_create_job(self, api):
        with self._mock_kazoo_client() as zk, self._mock_utcnow() as un:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            raw, _ = zk.get('/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id))
            meta_data = json.loads(raw.decode())
            assert meta_data["job_id"] == job_id
            assert meta_data["user_id"] == TEST_USER
            assert meta_data["status"] == "created"
            assert meta_data["api_version"] == api.api_version
            assert meta_data["application_id"] == None
            assert meta_data["created"] == "2020-04-20T16:04:03Z"

    def test_create_and_get(self, api):
        with self._mock_kazoo_client() as zk, self._mock_utcnow() as un:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

        if api.api_version_compare.at_least("1.0.0"):
            expected = {
                "id": job_id,
                "process": {"process_graph": self.DUMMY_PROCESS_GRAPH},
                "status": "created",
                "created": "2020-04-20T16:04:03Z"
            }
        else:
            expected = {
                "id": job_id,
                "process_graph": self.DUMMY_PROCESS_GRAPH,
                "status": "submitted",
                "submitted": "2020-04-20T16:04:03Z"
            }
        assert res == expected

    def test_get_legacy_zk_data(self, api):
        with self._mock_kazoo_client() as zk:
            job_id = 'ad597b92-e6f3-4241-88ce-d31739a740ff'
            raw = {
                'api_version': '0.4.0',
                'application_id': None,
                'job_id': job_id,
                'specification': '{"process_graph": {"foo": {"process_id": "foo", "arguments": {}}},'
                                 '"title": null, "description": null, "plan": null, "budget": null}',
                'status': 'submitted',
                'user_id': TEST_USER
            }
            zk.create(
                path='/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id),
                value=json.dumps(raw).encode(),
                makepath=True
            )

            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

        if api.api_version_compare.at_least("1.0.0"):
            expected = {
                "id": job_id,
                "process": {"process_graph": self.DUMMY_PROCESS_GRAPH,
                            "title": None, "description": None, "plan": None, "budget": None},
                "status": "created",
            }
        else:
            expected = {
                "id": job_id,
                "process_graph": self.DUMMY_PROCESS_GRAPH,
                "status": "submitted",
            }
        assert res == expected

    def test_create_and_get_user_jobs(self, api):
        with self._mock_kazoo_client() as zk, self._mock_utcnow() as un:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            created = "created" if api.api_version_compare.at_least("1.0.0") else "submitted"
            assert result == {
                "jobs": [
                    {"id": job_id, "status": created, created: "2020-04-20T16:04:03Z"},
                ],
                "links": []
            }

    def test_create_and_start_and_download(self, api, tmp_path):
        with self._mock_kazoo_client() as zk, \
                self._mock_utcnow() as un, \
                mock.patch.object(GpsBatchJobs, '_get_job_output_dir') as get_job_output_dir:
            get_job_output_dir.return_value = tmp_path

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    '/jobs/{j}/results'.format(j=job_id), json={},
                    headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()
                batch_job_args = run.call_args[0][0]

            # Check batch in/out files
            job_input = (tmp_path / "in")
            job_output = (tmp_path / "out")
            job_log = (tmp_path / "log")
            assert batch_job_args[2] == str(job_input)
            assert batch_job_args[3] == str(job_output)
            assert batch_job_args[4] == str(job_log)
            job_pg = load_json(job_input)
            assert job_pg["process_graph"] == self.DUMMY_PROCESS_GRAPH

            # Check metadata in zookeeper
            raw, _ = zk.get('/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id))
            meta_data = json.loads(raw.decode())
            assert meta_data["job_id"] == job_id
            assert meta_data["user_id"] == TEST_USER
            assert meta_data["status"] == "created"
            assert meta_data["api_version"] == api.api_version
            assert meta_data["application_id"] == 'application_1587387643572_0842'
            assert meta_data["created"] == "2020-04-20T16:04:03Z"
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "created" if api.api_version_compare.at_least("1.0.0") else "submitted"

            # Fake update from job tracker
            with openeogeotrellis.job_registry.JobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status="running")
            raw, _ = zk.get('/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id))
            meta_data = json.loads(raw.decode())
            assert meta_data["status"] == "running"
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "running"

            # Try to download results too early
            res = api.get('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER)
            res.assert_error(status_code=400, error_code='JobNotFinished')

            # Set up fake output and finish
            with job_output.open('wb') as f:
                f.write(TIFF_DUMMY_DATA)
            with job_log.open('w') as f:
                f.write("[INFO] Hello world")
            with openeogeotrellis.job_registry.JobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status="finished")
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "finished"

            # Download
            res = api.get(
                '/jobs/{j}/results'.format(j=job_id),
                headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            if api.api_version_compare.at_least("1.0.0"):
                download_url = res["assets"]["out"]["href"]
            else:
                download_url = res["links"][0]["href"]
            res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
            assert res.status_code == 200
            assert res.data == TIFF_DUMMY_DATA

            # Get logs
            res = api.get(
                '/jobs/{j}/logs'.format(j=job_id),
                headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            assert res["logs"] == [{"id": "0", "level": "error", "message": "[INFO] Hello world"}]

    def test_cancel_job(self, api, tmp_path):
        with self._mock_kazoo_client() as zk, \
                mock.patch.object(GpsBatchJobs, '_get_job_output_dir') as get_job_output_dir:
            get_job_output_dir.return_value = tmp_path

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    '/jobs/{j}/results'.format(j=job_id), json={},
                    headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()

            # Fake running
            with openeogeotrellis.job_registry.JobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status="running")
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "running"

            # Cancel
            with mock.patch('subprocess.run') as run:
                res = api.delete('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER)
                res.assert_status_code(204)
                run.assert_called_once()
                command = run.call_args[0][0]
                assert command == ["yarn", "application", "-kill", 'application_1587387643572_0842']
