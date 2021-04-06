from setuptools import setup, find_packages

def get_long_description():
    with open('README.md') as f:
        return f.read()

setup(
    name='sql-runner',
    version='0.4.10',

    description="DEPT SQL runner",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    python_requires='~=3.6',

    install_requires=[
        'networkx==2.5',
        'pydot==1.4.1',
        'graphviz==0.10.1',
        'pythondialog',
        'sqlparse',
    ],

    dependency_links=[
    ],

    extras_require={
        's3': ['boto3<1.11.0,>=1.4.4'],
        'snowflake': [
            'snowflake-connector-python==2.0.4',
            # Pin idna because requests installs a newer version than what is required by snowflake-connector-python
            'idna<2.9,>=2.5'
        ],
        'redshift': ['psycopg2-binary'],
        'postgres': ['psycopg2-binary'],
        'azuredwh': ['pyodbc'],
        'bigquery': ['google-cloud-bigquery==1.23.1']
    },

    packages=find_packages(),

    author='sql-runner contributors',
    license='Apache 2.0',

    entry_points={
        'console_scripts': [
            'runner = sql_runner.runner:main',
            # legacy
            'sqlrunner = sql_runner.runner:main',
            # Interactive
            'run_sql = sql_runner.run_sql:main'
        ],
    }
)
