import yaml
import pytest
import openapi3
from aiopenapi3 import OpenAPI

from slurmrest import improve


@pytest.fixture(scope="session")
def token(config):
    return improve.token(config["key"], config["user"])



@pytest.fixture(scope="session")
def config():
    cfg = yaml.load(open('config.yml', 'r'), Loader=yaml.Loader)
    return cfg




@pytest.fixture(scope="session")
def client(config, token):
    user = config["user"]
    headers = {"User-Agent": f"aiopenapi3+slurmrest/0.1.0"}
    import json, httpx
    def wget_factory(*args, **kwargs) -> httpx.Client:
        return improve.wget_factory(user, token, headers=headers)

    api = OpenAPI.load_sync(config["url"], session_factory=wget_factory,
                        plugins=[improve.OnDocument("v0.0.37"),
                                 improve.OnMessage()])

    def session_f(*args, **kwargs):
        h = kwargs.get("headers", dict()).copy()
        h.update(headers)
        kwargs["headers"] = h
        return httpx.Client(*args, **kwargs)
    api.wget_factory = session_f
    api.authenticate(user=user, token=token)
    api.info.version = "dbv0.0.37"
    return api


def test_slurmdbd_delete_account(client):
    test_slurmdbd_update_account(client)
    r = client._.slurmdbd_delete_account(parameters={"account_name":"unlimited"})
    assert r.errors == []


def test_slurmdbd_get_account(client):
    r = client._.slurmdbd_get_account(parameters={"account_name":"root"})
    assert r.errors == []
    assert len(r.accounts) > 0
    assert r.accounts[0].name == "root"


def test_slurmdbd_update_account(client):
    update_account = client._.slurmdbd_update_account.data.get_type()
    account = update_account.__fields__["accounts"].type_
    association_short_info = account.__fields__["associations"].type_

    a = account(
        name="unlimited",
        associations=[association_short_info(account='unlimited', cluster='c0', partition=None, user=None)],
        coordinators=[],
        description="unlimited resources",
        organization="unlimited",
        flags=[]
    )

    accounts = update_account(accounts=[a])

    r = client._.slurmdbd_update_account(data=accounts)
    assert r.errors == []


def test_slurmdbd_get_accounts(client):
    r = client._.slurmdbd_get_accounts()
    assert r.errors == []

@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_delete_association():
    raise NotImplementedError("slurmdbd_delete_association")


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_association():
    raise NotImplementedError("slurmdbd_get_association")


def test_slurmdbd_get_associations(client):
    r = client._.slurmdbd_get_associations()
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
    r = client._.slurmdbd_get_clusters()
    assert r.errors == []


def test_slurmdbd_get_db_config(client):
    r = client._.slurmdbd_get_db_config()
    assert r.errors == [] or r.errors[0].error == 'Nothing found with query'


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_set_db_config():
    raise NotImplementedError("slurmdbd_set_db_config")


def test_slurmctld_diag(client):
    r = client._.slurmctld_diag()
    assert r.errors == []


def test_slurmdbd_diag(client):
    r = client._.slurmdbd_diag()
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
    job_submission = client._.slurmctld_submit_job.data.get_type()
    job_properties = job_submission.__fields__["job"].type_

    j = job_properties(
        account="root",
        partition="debug",
        array="",
        ntasks=1,
        name="testing",
        nodes=[0, 0],
        current_working_directory="/tmp/",
        environment={}
    )

    # environment is defined object of type dict, invalid and can not be set with pydantic
    j.environment = {
        "PATH": "/bin:/usr/bin/:/usr/local/bin/",
        "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib"
    }

    j = job_submission(job=j, script="#!/bin/bash\nsrun echo it works")

    data = j.dict(exclude_unset=True) #exclude={"job": {"argv"}})
    r = client._.slurmctld_submit_job(data=data)
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmctld_update_job():
    raise NotImplementedError("slurmctld_update_job")


def test_slurmctld_get_jobs(client):
    r = client._.slurmctld_get_jobs()
    assert r.errors == []


def test_slurmdbd_get_jobs(client):
    r = client._.slurmdbd_get_jobs()
    assert r.errors == [] or r.errors[0].dict(exclude_unset=True) == {'error': 'Nothing found with query', 'error_number': 9003, 'source': 'slurmdb_jobs_get', 'description': 'Nothing found'}


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmctld_get_node():
    raise NotImplementedError("slurmctld_get_node")


def test_slurmctld_get_nodes(client):
    r = client._.slurmctld_get_nodes()
    assert r.errors == []


def test_slurmctld_get_partition(client):
    r = client._.slurmctld_get_partition(parameters={"partition_name":"debug"})
    assert r.errors == []
    assert len(r.partitions) > 0


def test_slurmctld_get_partitions(client):
    r = client._.slurmctld_get_partitions()
    assert r.errors == []


def test_slurmctld_ping(client):
    r = client._.slurmctld_ping()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_delete_qos():
    raise NotImplementedError("slurmdbd_delete_qos")


def test_slurmdbd_get_qos(client):
    r = client._.slurmdbd_get_qos()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_get_single_qos():
    raise NotImplementedError("slurmdbd_get_single_qos")


def test_slurmdbd_get_tres(client):
    r = client._.slurmdbd_get_tres()
    assert r.errors == []


@pytest.mark.xfail(raises=NotImplementedError)
def test_slurmdbd_update_tres():
    raise NotImplementedError("slurmdbd_update_tres")


def test_slurmdbd_delete_user(client):
    username = "c01teus"
    r = client._.slurmdbd_delete_user(parameters={"user_name":username})
    assert r.errors == []


def test_slurmdbd_get_user(client):
    username = "c01teus"
    r = client._.slurmdbd_get_user(parameters={"user_name":username})
    assert r.errors == []
    assert len(r.users) == 1
    assert r.users[0].name == username


def test_slurmdbd_get_users(client):
    r = client._.slurmdbd_get_users()
    assert r.errors == []
    assert len(r.users) > 0


def test_slurmdbd_update_users(client):
    username = "c01teus"

    s = client._.slurmdbd_update_users.data
    update_users = s.get_type()
    user = update_users.__fields__["users"].type_
    default_settings = user.__fields__["default"].type_
    association_short_info = user.__fields__["associations"].type_

    a = association_short_info(
        account="testing",
        cluster='c0',
        partition=None,
        user=username,
        usage=None)

    u = user(name=username,
        default=default_settings(account="testing"),
        associations=[a],
        coordinators=[])

    m = update_users(users=[u])
    r = client._.slurmdbd_update_users(data=m)
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
    r = client._.slurmdbd_get_wckeys()
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
