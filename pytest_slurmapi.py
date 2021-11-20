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
    return s.get(url)

@pytest.fixture(scope="session")
def spec(config, token):
    return wget(config["url"], config["user"], token).json()

@pytest.fixture(scope="session")
def config():
    cfg = yaml.load(open('config.yml', 'r'), Loader=yaml.Loader)
    return cfg

@pytest.fixture
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
    assert r == set()


def test_slurmctld_ping(client):
    r = client.call_slurmctld_ping()
    print(r)

def test_slurmdbd_get_users(client):
    r = client.call_slurmdbd_get_users()
    assert len(r.users) > 0

def test_slurmdbd_get_user(client):
    r = client.call_slurmdbd_get_user(parameters={"user_name":"root"})
    assert len(r.users) > 0

def test_slurmdbd_update_users(client):
    username = "c01teus"

    association = client.components.schemas["dbv0.0.36_association"].model(
        data={'account': "testing", 'cluster': 'c0', 'partition': None, 'user': username})

    user = client.components.schemas['dbv0.0.36_user'].model(
        data = {
            "name": username,
              "default": {"account": "testing"}, "associations": [association._raw_data],
              "coordinators": []})
    users = client.components.schemas['dbv0.0.36_update_users'].model(data={"users":[user._raw_data]})

    r = client.call_slurmdbd_update_users(data=users)
