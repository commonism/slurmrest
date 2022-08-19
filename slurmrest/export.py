import argparse
import collections
import re
import base64

from aiopenapi3 import OpenAPI
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from prometheus_client import Enum

from slurmrest import improve

improve.OnDocument._root = None
improve.OnMessage._root = None

def client(user, url, token):
    headers = {"User-Agent": f"aiopenapi3+slurmrest/0.1.0"}
    import json, httpx
    def wget_factory(*args, **kwargs) -> httpx.Client:
        return improve.wget_factory(user, token, headers=headers)

    api = OpenAPI.load_sync(url, session_factory=wget_factory,
                        plugins=[improve.OnDocument("v0.0.37"),
                                 improve.OnMessage()]
    )

    def session_f(*args, **kwargs):
        h = kwargs.get("headers", dict()).copy()
        h.update(headers)
        kwargs["headers"] = h
        return httpx.Client(*args, **kwargs)
    api.wget_factory = session_f
    api.authenticate(user=user, token=token)
    api.info.version = "dbv0.0.37"
    return api


def connect(user, key, url):
    token = improve.token(base64.b64encode(key), user)
    return client(user, url, token)


class Resource:
    def __init__(self, name, registry):
        self.used = Gauge(f'slurmctld_{name}_used_count', f'{name} allocation tracking', labelnames=["node"], registry=registry)
        self.total = Gauge(f'slurmctld_{name}_total_count', f'{name} resource tracking', labelnames=["node"], registry=registry)


class Export:
    STATES = ['drained', 'draining', 'idle', 'mixed', "allocated", "down"]

    def __init__(self):
        self.registry = registry = CollectorRegistry()
        self.state_value = Gauge('slurmctld_node_state_value', 'the state of the node', labelnames=["node"],
                                 registry=registry)
        self.state_name = Enum('slurmctld_node_state_name', 'the state of the node', labelnames=["node"],
                               states=self.STATES, registry=registry)

        self.values = {
            "^gres/((?P<model>[^\+])/)?gpu": Resource("gpu", registry),
            "^cpu": Resource("cpu", registry)
        }


def main():
    parser = argparse.ArgumentParser("slurmrest metrics exporter")
    parser.add_argument("--user", "-u", default="root")
    parser.add_argument("--jwt-key-file", "-j", default="/etc/slurm/jwt_hs256.key")
    parser.add_argument("--jwt-key", "-J")
    parser.add_argument("--url", "-U", default="http://127.0.0.1:6820/openapi.json")
    parser.add_argument("--outfile","-o", default="/var/lib/prometheus/node-exporter/slurmrest.prom")

    args = parser.parse_args()

    if not args.jwt_key:
        with open(args.jwt_key_file, "rb") as f:
            key = f.read()
    else:
        key = base64.b64decode(args.jwt_key)

    e = Export()
    client = connect(args.user, key, args.url)
    r = client._.slurmctld_get_nodes()
    assert r.errors == []

    used_ = collections.defaultdict(lambda: collections.defaultdict(lambda: list()))
    total_ = collections.defaultdict(lambda: collections.defaultdict(lambda: list()))

    for i in r.nodes:
        s = i.state
        if "DRAIN" in i.state_flags:
            if i.state == "idle":
                s = "drained"
            else:
                s = "draining"

        e.state_name.labels(i.name).state(s)
        e.state_value.labels(i.name).set(Export.STATES.index(s))

        if s == "drained":
            continue

        for name,data in {"tres":total_, "tres_used":used_}.items():
            value = getattr(i, name)
            if value is None:
                continue
            for k, v in dict(map(lambda y: (y[0], y[2]), map(lambda x: x.partition("="), value.split(",")))).items():
                data[i.name][k].append(v)

    for pattern,resource in e.values.items():
        for data,prom in [(used_,resource.used), (total_,resource.total)]:
            for node, sub in data.items():
                for res, val in sub.items():
                    m = re.match(pattern, res)
                    if not m:
                        continue
                    prom.labels(node).set(sum(map(float, val)))
                    break

    write_to_textfile(args.outfile, e.registry)


if __name__ == "__main__":
    main()
