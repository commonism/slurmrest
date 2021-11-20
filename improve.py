import argparse
import itertools
import json

import requests
from pathlib import Path


def apply(spec, version, live=''):
    def path(name):
        return f'{live}/{name}' + ('/' if not live else '')


    # all

    # openapi3.errors.SpecError: Could not parse security.0, expected to be one of [['SecurityRequirement']]
    spec["security"] = [{"user": []}, {"token": []}]

    spec['components']['schemas'].update({
        f"{version}_meta": {
            "type": "object",
            "properties": {
                "plugin": {
                    "$ref": f"#/components/schemas/{version}_plugin",
                },
                "Slurm": {
                    "type": "object",
                    "description": "Slurm information",
                    "properties": {
                        "version": {
                            "$ref": f"#/components/schemas/{version}_slurmversion"
                        },
                        "release": {
                            "type": "string",
                            "description": "version specifier"
                        }
                    }
                }
            }
        },
        f"{version}_plugin": {
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
        f"{version}_slurmversion": {
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
        }})

    for k, v in spec['components']['schemas'].items():
        if not "properties" in v:
            print(f"{k} {v}")
            continue

        if k in set([f"{version}_pings",
                     f"{version}_job_submission",
                     f"{version}_response_user_update",
                     f"{version}_user_info"]):
            v["properties"].update({"meta": {"$ref": f"#/components/schemas/{version}_meta"}, })


    if version.startswith("v"):
        # ctld
        del spec['components']['schemas'][f'{version}_job_submission']['properties']['job']['description']
    else:
        # dbd
        spec['components']['schemas'][f"{version}_user"]["properties"]["associations"] = \
            {
                "type": "array",
                "description": "the associations",
                "items": {
                    "$ref": "#/components/schemas/dbv0.0.36_association_short_info"
                }
            }

        # /users/
        spec['paths'][path('users')]['post'].update(
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


        if False:
            spec['paths'][path('users')]['post'].update({
            "requestBody": {
                "description": "update user",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "array",
                            "items": {
                                "$ref": f"#/components/schemas/{version}_user"
                            }
                        }
                    },
                    "application/x-yaml": {
                        "schema": {
                            "type": "array",
                            "items": {
                                "$ref": f"#/components/schemas/{version}_user"
                            }
                        }
                    }
                },
                "required": True
            },
        })

    return spec


def create_parser():
    parser = argparse.ArgumentParser("SLURMrestd API testing", description="…")

    sub = parser.add_subparsers()

    cmd = sub.add_parser("get")
    cmd.add_argument("--out", default="data/src")

    def cmd_get(args):
        for name, version in itertools.product(["dbv", "v"], list(f'0.0.{i}' for i in range(36, 39))):
            url = f"https://raw.githubusercontent.com/SchedMD/slurm/master/src/plugins/openapi/{name}{version}/openapi.json"
            out = Path(args.out) / f"{name}{version}.json"
            if not (p := out.parent).exists():
                p.mkdir(parents=True)
            r = requests.get(url)
            out.open("wt").write(r.text)

    cmd.set_defaults(func=cmd_get)

    cmd = sub.add_parser("patch")
    cmd.add_argument("--old", default="data/src")
    cmd.add_argument("--new", default="data/dst")

    def cmd_patch(args):
        if not (p := Path(args.new)).exists():
            p.mkdir(parents=True)

        for i in Path(args.old).iterdir():
            data = json.loads(i.open("r").read())
            data = apply(data, i.stem)
            (Path(args.new) / i.name).open('wt').write(json.dumps(data, indent=2))

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
