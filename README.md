## LEROI SQL runner

The LEROI SQL runner has three basic functionalities

* executing SQL code in a specific order
```
sqlrunner --execute {RUNNER_FILE_1}, {RUNNER_FILE_2} ..
```
* quickly testing SQL code through temporary creation of views
```
sqlrunner --test {RUNNER_FILE_1}, {RUNNER_FILE_2} ..
```
* plotting of a dependency graph
```
sqlrunner --deps
```

The supported databases are Redshift, Snowflake and Postgres.
### Installation
Pull repository and install dependencies
```sh
sudo apt install python3-pip
sudo apt install graphviz
pip install git+https://github.com/leroi-marketing/sql-runner.git[s3,azuredwh] #other optional dependencies
```
Additionally for Azure DWH, it's required to install the [Microsoft ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017). For Ubuntu 18.04 this is sufficient:
```sh
sudo curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install msodbcsql17
sudo apt-get install unixodbc-dev
```

### Configuration
Two configuration files are needed to use the sqlrunner.
* A config.json file that specifies all the necessary configuration variables.
```
{
   "sql_path": "{PATH}",
    "database_type": "{SNOWFLAKE} OR {REDSHIFT} OR {POSTGRES}",
    "auth": {
    "user": "{USERNAME}",
    "password": "{PASSWORD}",
    "account": "{SNOWFLAKE_ACCOUNT}",
    "database": "{SNOWFLAKE_DATABASE}",
    "dbname": "{POSTGRES_DATABASE} OR {REDSHIFT_DATABASE}",
    "host": "{POSTGRES_HOSTNAME} OR {REDSHIFT_HOSTNAME}",
    "port": "{POSTGRES_PORT} OR {REDSHIFT_PORT}"
     
   },
    "deps_schema": "{DEPENDENCY_SCHEMA_NAME}",
    "test_schema_prefix" : "{PREFIX_}",
    "exclude_dependencies": "('EXCLUDED_SCHEMA_1', 'EXCLUDED_SCHEMA_2' ..)",
    "graphviz_path": "{GRAPHVIZ_PATH_FOR_WINDOWS}"
}
```
* One or more csv files specifying the name of the the tables and views and their respective schemas.
 ```
 {SCHEMA_1};{SQL_FILENAME_1};e
 {SCHEMA_1};{SQL_FILENAME_2};e
 {SCHEMA_1};{SQL_FILENAME_3};e
 {SCHEMA_2};{SQL_FILENAME_4};e
 {SCHEMA_3};{SQL_FILENAME_5};e
 ..
 ```
Per schema one directory is expected. The name of the SQL files should correspond to thename of the respective table or view. The last columns specifies the desired action.
 ```
 e: execute the query
 t: create table
 v: create view
 m: materialize view
 ```

### Development

To set up dependencies locally for development:
```sh
# Install virtualenv (if your default python is python2, specify also `-p python3`)
virtualenv venv
source venv/bin/activate
pip install -e .[s3,azuredwh] # and other optional dependencies

# Run local (non-build) version:
python debug.py [arg1 arg2 ...]
```
