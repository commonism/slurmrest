import yaml
import pytest
import requests
import base64
import openapi3
from openapi3 import OpenAPI


import improve

@pytest.fixture(scope="session")
def token(config):
    priv_key = base64.b64decode(config["key"])
    interval = 600
    user = config["user"]

    import time

    from jwt import JWT
    from jwt.jwk import jwk_from_dict
    from jwt.utils import b64encode

    signing_key = jwk_from_dict({
        'kty': 'oct',
        'k': b64encode(priv_key)
    })

    message = {
        "exp": int(time.time() + interval),
        "iat": int(time.time()),
        "sun": user
    }

    a = JWT()
    compact_jws = a.encode(message, signing_key, alg='HS256')
    return compact_jws

def _session_factory(user, token):
    s = requests.Session()
    s.headers.update({
        "X-SLURM-USER-NAME": user,
        "X-SLURM-USER-TOKEN": token,
    })
    return s

def wget(url, user, token):
    s = _session_factory(user, token)
    r = s.get(url)
    return r

@pytest.fixture(scope="session")
def spec(config, token):
    return wget(config["url"], config["user"], token).json()

@pytest.fixture(scope="session")
def config():
    cfg = yaml.load(open('config.yml', 'r'), Loader=yaml.Loader)
    return cfg

@pytest.fixture(scope="session")
def client(config, token, spec):
    user = config["user"]
    _v = spec['info']['version'].split('v')[1]
    for i in ["","db"]:
        improve.apply(spec, f"{i}v{_v}", f"/slurm{i}/v{_v}")


    spec["servers"][0]["url"] = "http://127.0.0.1:6820" + spec["servers"][0]["url"]

    api = OpenAPI(spec, session_factory=lambda: _session_factory(config['user'], token), use_session=True)
    # api.authenticate does not work for multi values
    api._security = {'user': user, 'token': token}
    return api


def test_slurmdbd_delete_account(client):
    test_slurmdbd_update_account(client)
    r = client.call_slurmdbd_delete_account(parameters={"account_name":"unlimited"})
    assert r.errors == []


def test_slurmdbd_get_account(client):
    r = client.call_slurmdbd_get_account(parameters={"account_name":"root"})
    assert r.errors == []
    assert len(r.accounts) > 0
    assert r.accounts[0].name == "root"


def test_slurmdbd_update_account(client):
    account = client.components.schemas["dbv0.0.36_account"].model(
        data={"name": "unlimited",
              "associations": [{'account': 'unlimited', 'cluster': 'c0', 'partition': None, 'user': None}],
              "coordinators": [],
              "description": "unlimited resources",
              "organization": "unlimited",
              "flags": []
    })

    accounts = client.components.schemas['dbv0.0.36_update_account'].model(data={"accounts":[account._raw_data]})
    r = client.call_slurmdbd_update_account(data=accounts)
    assert r.errors == []


def test_slurmdbd_get_accounts(client):
    r = client.call_slurmdbd_get_accounts()
    assert r.errors == []

@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_delete_association():
    raise NotImplementedError("slurmdbd_delete_association")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_association():
    raise NotImplementedError("slurmdbd_get_association")


def test_slurmdbd_get_associations(client):
    r = client.call_slurmdbd_get_associations()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_delete_cluster():
    raise NotImplementedError("slurmdbd_delete_cluster")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_cluster():
    raise NotImplementedError("slurmdbd_get_cluster")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_add_clusters():
    raise NotImplementedError("slurmdbd_add_clusters")


def test_slurmdbd_get_clusters(client):
    r = client.call_slurmdbd_get_clusters()
    assert r.errors == []


def test_slurmdbd_get_db_config(client):
    r = client.call_slurmdbd_get_db_config()
    assert r.errors == [] or r.errors[0].error == 'Nothing found with query'


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_set_db_config():
    raise NotImplementedError("slurmdbd_set_db_config")


def test_slurmctld_diag(client):
    r = client.call_slurmctld_diag()
    assert r.errors == []


def test_slurmdbd_diag(client):
    r = client.call_slurmdbd_diag()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmctld_cancel_job():
    raise NotImplementedError("slurmctld_cancel_job")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmctld_get_job():
    raise NotImplementedError("slurmctld_get_job")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_job():
    raise NotImplementedError("slurmdbd_get_job")


def test_slurmctld_submit_job(client):
    job = {
        "job": {
            "account": "root",
            "partition": "debug",
            "array": "",
            "ntasks": 1,
            "name": "testing",
            "nodes": [0,0],
            "current_working_directory": "/tmp/",
            "environment": {
                "PATH": "/bin:/usr/bin/:/usr/local/bin/",
                "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib"
            }
        },
        "script": "#!/bin/bash\nsrun echo it works"
    }
    r = client.call_slurmctld_submit_job(data=job)
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmctld_update_job():
    raise NotImplementedError("slurmctld_update_job")


def test_slurmctld_get_jobs(client):
    r = client.call_slurmctld_get_jobs()
    assert r.errors == []


def test_slurmdbd_get_jobs(client):
    r = client.call_slurmdbd_get_jobs()
    assert r.errors == [] or r.errors[0]._raw_data == {'error': 'Nothing found with query', 'error_number': 9003, 'source': 'slurmdb_jobs_get', 'description': 'Nothing found'}


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmctld_get_node():
    raise NotImplementedError("slurmctld_get_node")


def test_slurmctld_get_nodes(client):
    r = client.call_slurmctld_get_nodes()
    assert r.errors == []


def test_slurmctld_get_partition(client):
    r = client.call_slurmctld_get_partition(parameters={"partition_name":"debug"})
    assert r.errors == []
    assert len(r.partitions) > 0

def test_slurmctld_get_partitions(client):
    r = client.call_slurmctld_get_partitions()
    assert r.errors == []

def test_slurmctld_ping(client):
    r = client.call_slurmctld_ping()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_delete_qos():
    raise NotImplementedError("slurmdbd_delete_qos")


def test_slurmdbd_get_qos(client):
    r = client.call_slurmdbd_get_qos()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_single_qos():
    raise NotImplementedError("slurmdbd_get_single_qos")


def test_slurmdbd_get_tres(client):
    r = client.call_slurmdbd_get_tres()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_update_tres():
    raise NotImplementedError("slurmdbd_update_tres")


def test_slurmdbd_delete_user(client):
    username = "c01teus"
    r = client.call_slurmdbd_delete_user(parameters={"user_name":username})
    assert r.errors == []

def test_slurmdbd_get_user(client):
    username = "c01teus"
    r = client.call_slurmdbd_get_user(parameters={"user_name":username})
    assert r.errors == []
    assert len(r.users) == 1
    assert r.users[0].name == username


def test_slurmdbd_get_users(client):
    r = client.call_slurmdbd_get_users()
    assert r.errors == []
    assert len(r.users) > 0

def test_slurmdbd_update_users(client):
    username = "c01teus"

    association = client.components.schemas["dbv0.0.36_association"].model(
        data={'account': "testing", 'cluster': 'c0', 'partition': None, 'user': username})

    user = client.components.schemas['dbv0.0.36_user'].model(
        data={
            "name": username,
            "default": {"account": "testing"},
            "associations": [association._raw_data],
            "coordinators": []
        })
    users = client.components.schemas['dbv0.0.36_update_users'].model(data={"users":[user._raw_data]})

    r = client.call_slurmdbd_update_users(data=users)
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_delete_wckey():
    raise NotImplementedError("slurmdbd_delete_wckey")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_wckey():
    raise NotImplementedError("slurmdbd_get_wckey")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_add_wckeys():
    raise NotImplementedError("slurmdbd_add_wckeys")


def test_slurmdbd_get_wckeys(client):
    r = client.call_slurmdbd_get_wckeys()
    assert r.errors == [] or r.errors[0].error == 'Nothing found with query'


def test_coverage(client):
    operations = list()
    for p in filter(lambda x: isinstance(x, openapi3.paths.Path), client.paths.values()):
        operations.extend(map(lambda y: y.operationId, filter(lambda x: x and x.operationId, [p.get, p.post, p.put, p.head, p.patch, p.options, p.delete])))
    print(operations)
    operations = set(operations)
    import sys
    import inspect

    tests = set(
        map(
            lambda x: x[len("test_"):],
            filter(
                lambda x: x.startswith("test_"),
                map(
                    lambda o: o[0],
                    inspect.getmembers(sys.modules[__name__], inspect.isfunction))
            )
        )
    )
    r = operations - tests

    with open("missing.txt", "wt") as f:
        for i in sorted(r, key=lambda x: x.split("_")[::-1]):
            f.write(f"""
def test_{i}():
    raise NotImplementedError("{i}")        

            """)
    assert r == set()
