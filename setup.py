from setuptools import setup, find_packages

def get_long_description():
    with open('README.md') as f:
        return f.read()

setup(
    name='sql-runner',
    version='0.1.0',

    description="LEROI SQL runner",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    python_requires='~=3.6',

    install_requires=[
        'azure-storage-blob==1.4.0',
        'psycopg2-binary==2.7.7',
        'snowflake-connector-python==1.7.4',
        'networkx==2.2',
        'boto3==1.9.75',
        'pydot==1.4.1',
        'graphviz==0.10.1'
    ],

    dependency_links=[
    ],

    #extras_require={
    #    'test': ['pytest', 'pytest_click'],
    #},

    packages=find_packages(),

    author='sql-runner contributors',
    license='Apache 2.0',

    entry_points={
        'console_scripts': [
            'runner = src.runner:main',
        ],
    }
)
