import argparse
import itertools
import json
import base64
import time
import re
from pathlib import Path

import httpx

import jmespath
from jwt import JWT
from jwt.jwk import jwk_from_dict
from jwt.utils import b64encode

import aiopenapi3.plugin


def token(key, user):
    priv_key = base64.b64decode(key)
    interval = 600
    user = user

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

def versionof(name):
    return re.match("(\D+)([\w.]+)", name).groups()


def wget_factory(user, token, *args, **kwargs):
    session = httpx.Client(*args, **kwargs)
    session.headers.update({
        "X-SLURM-USER-NAME": user,
        "X-SLURM-USER-TOKEN": token,
    })
    return session


def wget(url, user, token):
    s = wget_factory(user, token)
    r = s.get(url)
    return r


class OnDocument(aiopenapi3.plugin.Document):
    def __init__(self, version):
        super().__init__()
        self._version = version
    def parsed(self, ctx):
        spec = ctx.document
        for i in ["", "db"]:
            apply(spec, f"{i}{self._version}", f"/slurm{i}/{self._version}")
        return ctx


class OnMessage(aiopenapi3.plugin.Message):
    def parsed(self, ctx):
        if ctx.operationId == "slurmctld_get_jobs":
            data = ctx.parsed
            # job_resources
            for job in range(len(data["jobs"])):
                if 'allocated_nodes' not in data['jobs'][job]['job_resources']:
                    continue
                data['jobs'][job]['job_resources']['allocated_nodes'] = \
                    [{**i, "node": k} for k, i in data['jobs'][job]['job_resources']['allocated_nodes'].items()]

            # node_allocation
            for job in range(len(data["jobs"])):
                if 'allocated_nodes' not in data['jobs'][job]['job_resources']:
                    continue
                for node in range(len(data['jobs'][job]['job_resources']['allocated_nodes'])):
                    new = [
                        {"core": k, "type": v}
                        for k, v in data['jobs'][job]['job_resources']['allocated_nodes'][node]['cores'].items()
                    ]
                    data['jobs'][job]['job_resources']['allocated_nodes'][node]['cores'] = new
                for node in range(len(data['jobs'][job]['job_resources']['allocated_nodes'])):
                    new = [
                        {"socket": k, "type": v}
                        for k, v in data['jobs'][job]['job_resources']['allocated_nodes'][node]['sockets'].items()
                    ]
                    data['jobs'][job]['job_resources']['allocated_nodes'][node]['sockets'] = new
        return ctx


def apply(spec, version, live=''):

    _,v0 = versionof(version)

    # remove all oeprations besides the choosen one …
    for i in jmespath.search("paths|keys(@)", spec):
        if not len((p:=Path(i)).parts) > 2:
            # /openapi/…
            continue
        _, v1 = versionof(p.parts[2])
        if v0 == v1:
            continue
        del spec['paths'][i]

    # remove all components …
    for i in list(spec["components"]["schemas"].keys()):
        _, v1 = versionof(i.split("_")[0])
        if v0 != v1:
            del spec["components"]["schemas"][i]


    def operationof(name):

        if (r:= jmespath.search(f"paths.[*][].[*][][?operationId == '{name}'][]", spec)):
            return r[0]
        elif(r:= jmespath.search(f"""paths.[*][].[*][][?operationId == '{name.partition("_")[2]}'][]""", spec)):
            return r[0]
        raise KeyError(name)

    spec['components']['schemas'].update({
        f"{version}_meta": {
            "type": "object",
            "properties": {
                "plugin": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "",
                        },
                        "name": {
                            "type": "string",
                            "description": "",
                        },
                    }
                },
                "Slurm": {
                    "type": "object",
                    "description": "Slurm information",
                    "properties": {
                        "version": {
                            "type": "object",
                            "properties": {
                                "major": {
                                    "type": "string",
                                    "description": "",
                                },
                                "micro": {
                                    "type": "string",
                                    "description": "",
                                },
                                "minor": {
                                    "type": "string",
                                    "description": "",
                                }
                            }
                        },
                        "release": {
                            "type": "string",
                            "description": "version specifier"
                        }
                    }
                }
            }
        },
    })

    for k, v in spec['components']['schemas'].items():
        if not "properties" in v:
            print(f"{k} {v}")
            continue

        if k in set([f"{version}_pings",
                     f"{version}_job_submission",
                     f"{version}_response_user_update",
                     f"{version}_user_info",
                     f"{version}_account_info",
                     f"{version}_account_response",
                     f"{version}_response_account_delete",
                     f"{version}_jobs_response",
                     f"{version}_job_info",
                     f"{version}_nodes_response",
                     f"{version}_partitions_response",
                     f"{version}_diag",
                     f"{version}_job_submission_response",
                     f"{version}_tres_info",
                     f"{version}_associations_info",
                     f"{version}_cluster_info",
                     f"{version}_config_info",
                     f"{version}_qos_info",
                     f"{version}_wckey_info",
                     f"{version}_response_user_delete", # v0.0.37
                     ]):
            v["properties"].update({"meta": {"$ref": f"#/components/schemas/{version}_meta"}, })


    if version.startswith("v"):
        # ctld
        try:
            del spec['components']['schemas'][f'{version}_job_submission']['properties']['job']['description']
        except KeyError:
            pass
        spec['components']['schemas'][f"{version}_error"]["properties"].update({
            "error_code":{"type": "integer"},
            "error_number": {"type": "integer"},
            "description": {"type": "string"},
            "source": {"type": "string"},
            })


        # this is basically completely broken and not compatible to openapi
        # using dicts with dynamic index for lists

        spec['components']['schemas'][f'{version}_job_resources']['properties']["allocated_nodes"] = {
            "type": "array",
            "description": "node allocations",
            "items": {
                "$ref": f"#/components/schemas/{version}_node_allocation"
            }
        }

        spec['components']['schemas'][f'{version}_node_allocation']['properties']["cores"] = {
            "type":"array",
            "description":"FIXME",
            "items": {
                "type":"object",
                "properties": {
                    "core":{
                        "type":"integer",
                    },
                    "type": {
                        "type":"string"
                    }
                }
            }
        }
        spec['components']['schemas'][f'{version}_node_allocation']['properties']["sockets"] = {
            "type":"array",
            "description":"FIXME",
            "items": {
                "type":"object",
                "properties": {
                    "socket":{
                        "type":"integer",
                    },
                    "type": {
                        "type":"string"
                    }
                }
            }
        }
        spec['components']['schemas'][f'{version}_node_allocation']['properties']["cpus"] = {"type":"integer"}

        del spec['components']['schemas'][f"{version}_job_properties"]["properties"]["account_gather_freqency"]
    else:
        # dbd
        spec['components']['schemas'][f"{version}_user"]["properties"]["associations"] = \
            {
                "type": "array",
                "description": "the associations",
                "items": {
                    "$ref": f"#/components/schemas/{version}_association_short_info"
                }
            }

        # /users/
        operationof("slurmdbd_update_users").update(
            {
                "requestBody": {
                    "description": "update user",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_update_users"
                            }
                        },
                        "application/x-yaml": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_update_users"
                            }
                        }
                    },
                    "required": True
                },
            })

        operationof("slurmdbd_update_account").update(
            {
                "requestBody": {
                    "description": "update/create accounts",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_update_account"
                            }
                        },
                        "application/x-yaml": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_update_account"
                            }
                        }
                    },
                    "required": True
                },
            })

        spec['components']['schemas'][f"{version}_response_account_delete"]["properties"]["removed_associations"] = \
            {
                "type": "array",
                "description": "the associations",
                "items": {
                    "type": "string"
                }
            }




        # 400/500 error handling via default
        for i in jmespath.search("paths.[*][].[*][][].operationId", spec):
            for code,desc in {400:"Invalid Request",500:"Internal Error"}.items():
                operationof(i)["responses"].update(
                    {
                        f"{code}": {
                            "description": desc,
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": f"#/components/schemas/{version}_errors"
                                    }
                                },
                                "application/x-yaml": {
                                    "schema": {
                                        "$ref": f"#/components/schemas/{version}_errors"
                                    }
                                }
                            },
                        }
                    })


        spec['components']['schemas'].update({
            f"{version}_errors": {
                "properties": {
                    "meta":{"$ref": f"#/components/schemas/{version}_meta"},
                    "errors": {
                        "type": "array",
                        "description": "Slurm errors",
                        "items": {
                            "$ref": f"#/components/schemas/{version}_error"
                        }
                    },
                }
            }
        })



        spec['components']['schemas'][f"{version}_error"]["properties"].update({
            "error_number": {"type": "integer"},
            "source":{"type":"string"},
            "error_code":{"type": "integer"},
            "description": {"type": "string"},
            })


        # diag - 500 is not an error?
        operationof("slurmdbd_diag")["responses"].update(
            {
                "500": {
                    "description": "This should be wrong.",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_diag"
                            }
                        },
                        "application/x-yaml": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_diag"
                            }
                        }
                    },
                    "description": "Dictionary of statistics"
                }
            })

        properties = dict()
        for k in ["users", "RPCs", "rollups", "time_start"]:
            properties[k] = spec['components']['schemas'][f"{version}_diag"]["properties"][k]
            del spec['components']['schemas'][f"{version}_diag"]["properties"][k]
        spec['components']['schemas'][f"{version}_diag"]["properties"].update({
            "statistics": {"type": "object", "properties" : properties}
        })
        spec['components']['schemas'][f"{version}_diag"]["properties"]["statistics"]["properties"]["rollups"]["items"]["properties"]["total_cycles"] = { "type": "integer", "description": "magic value"}

        spec['components']['schemas'].update({
            f"{version}_update_users": {
                "properties": {
                    "users": {
                        "type": "array",
                        "items": {
                            "$ref": f"#/components/schemas/{version}_user"
                        }
                    }
                }
            }
        })

        spec['components']['schemas'].update({
            f"{version}_update_account": {
                "properties": {
                    "accounts": {
                        "type": "array",
                        "items": {
                            "$ref": f"#/components/schemas/{version}_account"
                        }
                    }
                }
            }
        })

        spec['components']['schemas'][f"{version}_tres_info"]["properties"]["TRES"] = {"$ref": f"#/components/schemas/{version}_tres_list"}
        del spec['components']['schemas'][f"{version}_tres_info"]["properties"]["tres"]

        # slurmdbd_get_associations
        spec['components']['schemas'][f"{version}_association"]["properties"]['max']['properties']['jobs']['properties']['per']['properties'].update({
            'accruing': {"type":"string"},
            'count': {"type":"integer"},
            'submitted': {"type":"string"},
        })

        # cluster
        spec['components']['schemas'][f"{version}_cluster"] = spec['components']['schemas'][f"{version}_cluster_info"].copy()

        spec['components']['schemas'][f"{version}_cluster"]["properties"]["associations"]["properties"]["root"] = {
            "$ref": f"#/components/schemas/{version}_association_short_info"
        }

        spec['components']['schemas'][f"{version}_cluster"]["properties"]["tres"] = {
            "$ref": f"#/components/schemas/{version}_tres_list"
        }

        del spec['components']['schemas'][f"{version}_cluster"]["properties"]["meta"]

        spec['components']['schemas'][f"{version}_cluster_info"]["properties"] = {
            "errors": {
                "type": "array",
                "description": "Slurm errors",
                "items": {
                    "$ref": f"#/components/schemas/{version}_error"
                }
            },
            "clusters": {"type": "array", "items":{"$ref": f"#/components/schemas/{version}_cluster"}},
            "meta": {"$ref": f"#/components/schemas/{version}_meta"},
        }

        # config_info
        spec['components']['schemas'][f"{version}_config_info"]["properties"]["clusters"] = {"type": "array", "items":{"$ref": f"#/components/schemas/{version}_cluster"}}
        spec['components']['schemas'][f"{version}_config_info"]["properties"]["TRES"] = {
            "type": "object",
            "$ref": f"#/components/schemas/{version}_tres_list",
        }
        del spec['components']['schemas'][f"{version}_config_info"]["properties"]["tres"]

        spec['components']['schemas'][f"{version}_config_info"]["properties"]["QOS"] = spec['components']['schemas'][f"{version}_config_info"]["properties"]["qos"]

        del spec['components']['schemas'][f"{version}_config_info"]["properties"]["qos"]

        spec['components']['schemas'][f"{version}_qos"]["properties"]["name"] = {"type": "string"}
        spec['components']['schemas'][f"{version}_qos"]["properties"]["limits"]["properties"]["grace_time"] = {"type": "integer"}
        spec['components']['schemas'][f"{version}_qos"]["properties"]["limits"]["properties"]["max"]["properties"]["active_jobs"] = {
            "type": "object",
            "properties": {
                "accruing":{
                    "type":"string"
                },
                "count":{
                    "type":"string"
                }
            }
        }
        spec['components']['schemas'][f"{version}_qos"]["properties"]["limits"]["properties"]["max"]["properties"]["tres"]["properties"]["total"] = {
            "type": "array",
            "items":{
                "type":"integer"
            }
        }

        spec['components']['schemas'][f"{version}_qos"]["properties"]["limits"]["properties"]["max"]["properties"]\
            ["tres"]["properties"]["minutes"]["properties"]["per"]["properties"]["qos"] = {"$ref": f"#/components/schemas/{version}_tres_list"}

        spec['components']['schemas'][f"{version}_qos_info"]["properties"]["QOS"] = spec['components']['schemas'][f"{version}_qos_info"]["properties"]["qos"]
        del spec['components']['schemas'][f"{version}_qos_info"]["properties"]["qos"]

        # wckey
        # diag - 500 is not an error?
        operationof("slurmdbd_get_wckeys")["responses"].update(
            {
                "500": {
                    "description": "This should be wrong.",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_wckey_info"
                            }
                        },
                        "application/x-yaml": {
                            "schema": {
                                "$ref": f"#/components/schemas/{version}_wckey_info"
                            }
                        }
                    },
                    "description": "List of wckeys"
                }
            })

        spec['components']['schemas'][f"{version}_job"]["properties"]["het"]["properties"]["job_id"]["type"] = "integer"
        spec['components']['schemas'][f"{version}_job"]["properties"]["het"]["properties"]["job_offset"]["type"] = "integer"
        spec['components']['schemas'][f"{version}_job_step"]["properties"].update({
            "task": {
                "type": "object",
                "properties": {
                    "distribution": {
                        "type":"string"
                    }
                }
            },
            "tres": {
                "type": "object",
                "description": "TRES usage",
                "properties": {
                  "requested": {
                    "type": "object",
                    "description": "TRES requested for job",
                    "properties": {
                      "average": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      },
                      "max": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      },
                      "min": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      },
                      "total": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      }
                    }
                  },
                  "consumed": {
                    "type": "object",
                    "description": "TRES requested for job",
                    "properties": {
                      "average": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      },
                      "max": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      },
                      "min": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      },
                      "total": {
                        "$ref": f"#/components/schemas/{version}_tres_list"
                      }
                    }
                  },
                  "allocated": {
                    "$ref": f"#/components/schemas/{version}_tres_list"
                  }
                }
            }
        })


    return spec


def create_parser():
    parser = argparse.ArgumentParser("SLURMrestd API testing", description="…")

    sub = parser.add_subparsers()

    cmd = sub.add_parser("get")
    cmd.add_argument("--out", default="data/src")

    def cmd_get(args):
        for name, version in itertools.product(["dbv", "v"], list(f'0.0.{i}' for i in range(37, 39))):
            url = f"https://raw.githubusercontent.com/SchedMD/slurm/master/src/plugins/openapi/{name}{version}/openapi.json"
            out = Path(args.out) / f"{name}{version}.json"
            if not (p := out.parent).exists():
                p.mkdir(parents=True)
            r = httpx.get(url)
            out.open("wt").write(r.text)

    cmd.set_defaults(func=cmd_get)

    cmd = sub.add_parser("patch")
    cmd.add_argument("--old", default="data/src")
    cmd.add_argument("--new", default="data/dst")
    cmd.add_argument("--slurm", default="~/workspace/slurm/")

    def cmd_patch(args):
        if not (p := Path(args.new)).exists():
            p.mkdir(parents=True)

        for i in Path(args.old).iterdir():
            data = json.loads(i.open("r").read())
            data = apply(data, i.stem)
            (Path(args.new) / i.name).open('wt').write((data:=json.dumps(data, indent=2)))

            if not (s:=Path(args.slurm).expanduser()).exists():
                continue
            (s / "src"/"plugins"/"openapi"/ i.stem / "openapi.json").open('wt').write(data)
#            name,version,_ = re.match("(\D+)([\w.]+)",i.stem).groups()

    cmd.set_defaults(func=cmd_patch)


    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_usage()


if __name__ == '__main__':
    main()
