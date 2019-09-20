from setuptools import setup, find_packages

def get_long_description():
    with open('README.md') as f:
        return f.read()

setup(
    name='sql-runner',
    version='0.2.0',

    description="LEROI SQL runner",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    python_requires='~=3.6',

    install_requires=[
        'networkx==2.2',
        'pydot==1.4.1',
        'graphviz==0.10.1'
    ],

    dependency_links=[
    ],

    extras_require={
        's3': ['boto3==1.9.75'],
        'snowflake': ['snowflake-connector-python==1.7.4', 'azure-storage-blob==1.4.0'],
        'redshift': ['psycopg2-binary==2.7.7'],
        'postgres': ['psycopg2-binary==2.7.7'],
        'azuredwh': ['pyodbc']
    },

    packages=find_packages(),

    author='sql-runner contributors',
    license='Apache 2.0',

    entry_points={
        'console_scripts': [
            'runner = src.runner:main',
        ],
    }
)
