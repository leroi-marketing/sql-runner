from setuptools import setup, find_packages

def get_long_description():
    with open('README.md') as f:
        return f.read()

setup(
    name='sql-runner',
    version='0.5.0',

    description="DEPT SQL runner",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    python_requires='~=3.8',

    install_requires=[
        'networkx==2.5',
        'pydot==1.4.2',
        'graphviz==0.16',
        'pythondialog',
        'sqlparse',
    ],

    dependency_links=[
    ],

    extras_require={
        's3': ['boto3==1.17.33'],
        'snowflake': [
            'snowflake-connector-python==2.4.1',
        ],
        'redshift': ['psycopg2-binary'],
        'postgres': ['psycopg2-binary'],
        'azuredwh': ['pyodbc'],
        'bigquery': ['google-cloud-bigquery==2.12.0'],
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
            'run_sql = sql_runner.run_sql:main',
        ],
    }
)
