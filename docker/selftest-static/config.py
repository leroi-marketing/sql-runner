import boto3
from botocore.exceptions import ClientError
import json


class Config:
    sql_path = "/app/sql"
    database_type = "snowflake"
    explicit_database = True
    test = {
      "override": {
        "schema": {
          "prefix": "zz_"
        }
      },
      "except": "re.search('^x', schema)"
    }
    deps_schema = "monitor"
    s3_bucket = False
    s3_folder = False
    exclude_dependencies = [
      "information_schema"
    ]
    colors = {
        "y_": "#babaff",
        "x": "#ddddff",
        "dev_": "#ddffdd",
        "star": "#ffffdd",
        "rep_": "#507a93"
    }
    shapes = {
        "y_": "box",
        "x": "box",
        "star": "hexagon",
        "rep_": "hexagon"
    }

    auth = {
        "user": "DEPT_TEAM",
        "password": "123456",
        "database": "DWH",
        "account": "db"
    }


if __name__ == '__main__':
    import json
    config = {}
    config_obj = Config()
    for key in dir(config_obj):
        if not key.startswith('__'):
            config[key] = getattr(config_obj, key)
    print(json.dumps(config, indent=4))
