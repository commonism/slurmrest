import dataclasses

import yaml
import collections
import re

from aiopenapi3 import OpenAPI
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from prometheus_client import Enum

import improve

def client(config, token):
    user = config["user"]
    headers = {"User-Agent": f"aiopenapi3+slurmrest/0.1.0"}
    import json, httpx
    def wget_factory(*args, **kwargs) -> httpx.Client:
        return improve.wget_factory(user, token, headers=headers)

    api = OpenAPI.load_sync(config["url"], session_factory=wget_factory,
#                        plugins=[improve.OnDocument("v0.0.37"),
#                                 improve.OnMessage()]
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


def connect():
    config = yaml.load(open('../config.yml', 'r'), Loader=yaml.Loader)
    token = improve.token(config["key"], config["user"])
    return client(config, token)


class Export:
    STATES = ['drained', 'draining', 'idle', 'mixed', "allocated"]
    def __init__(self):
        self.registry = registry = CollectorRegistry()
        self.state_value = Gauge('slurmctld_node_state_value', 'the state of the node', labelnames=["node"], registry=registry)
        self.state_name = Enum('slurmctld_node_state_name', 'the state of the node', labelnames=["node"], self.STATES, registry=registry)
        self.used = Gauge('slurmctld_gpu_used_count', 'gpu allocation tracking', labelnames=["node"], registry=registry)
        self.total = Gauge('slurmctld_gpu_total_count', 'gpu resource tracking', labelnames=["node"], registry=registry)

def main():
    e = Export()
    client = connect()
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
        e.state_vate.labels(i.name).set(Export.STATES.index(s))

        if s == "drained":
            continue

        for name,data in {"tres":total_, "tres_used":used_}.items():
            value = getattr(i, name)
            if value is None:
                continue
            for k, v in dict(map(lambda y: (y[0], y[2]), map(lambda x: x.partition("="), value.split(",")))).items():
                data[i.name][k].append(v)


    for data,prom in [(used_,e.used), (total_,e.total)]:
        for node,sub in data.items():
            for res,val in sub.items():
                m = re.match("^gres/((?P<model>[^\+])/)?gpu", res)
                if m:
                    prom.labels(node).set(sum(map(float, val)))


    FILE='/tmp/raid.prom'
    write_to_textfile(FILE, e.registry)
    print(open(FILE).read())

if __name__ == "__main__":
    main()