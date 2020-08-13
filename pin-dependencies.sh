#!/usr/bin/env bash

set -eo pipefail

# Pin dependencies in order to replicate installs

cleanup() {
    echo "Cleaning up"
    if [ -d temp_venv ] ; then
        rm -rf temp_venv
    fi

    if [ -d docker/s3 ] ; then
        rm -rf docker/s3
    fi
    if [ -f requirements.txt ] ; then
        rm requirements.txt
    fi
}

trap cleanup EXIT

pass() {
    echo "" > /dev/null
}

py() {
    deactivate 2> /dev/null || pass
    if python --version | grep "Python 3." > /dev/null ; then
        PYTHON="$(which python)"
    elif which python3.6 ; then
        PYTHON="$(which python3.6)"
    elif which python3 ; then
        PYTHON="$(which python3)"
    fi

    if ! $PYTHON <<~
import platform
python_version = list(map(int, platform.python_version().split('.')))
if python_version[:3] >= [3, 6, 0]:
    exit(0)
else:
    exit(1)
~
    then
        echo "Python 3.6 or later required"
        exit 1
    fi
    echo $PYTHON
}

getdeps() {
    EXTRA=$1
    if [ ! "$1" ] ; then
        echo "Installing base virtualenv"
        deactivate 2> /dev/null || pass
        virtualenv -p $(py) temp_venv #> /dev/null
        source temp_venv/bin/activate
        pip install -e . #> /dev/null
        pip freeze | grep -v "egg=sql_runner" > requirements.txt
    fi
    if [ "$VIRTUAL_ENV" != "$(pwd)/temp_venv" ] ; then
        source temp_venv/bin/activate
    fi
    if [ "$EXTRA" ] ; then
        echo "Installing dependencies for $EXTRA"
        pip install -e .[$EXTRA] #> /dev/null
        echo "Saving requirements"
        pip freeze | tee requirements-$EXTRA.txt
        sleep 1
        echo "Making a diff"
        mkdir -p docker/$EXTRA
        cp requirements.txt docker/$EXTRA/
        OUTFILE="docker/$EXTRA/requirements-extra.txt"
        diff requirements.txt requirements-$EXTRA.txt | grep --color=never -oPie "(?<=^> ).*$" | grep -v "egg=sql_runner" > $OUTFILE || pass
        rm requirements-$EXTRA.txt
        echo "Uninstalling extra requirements"
        while read LINE ; do
            pip uninstall -y $LINE
        done < $OUTFILE
    fi
}

getdeps
while read  LINE ; do
    echo "Saving dependencies for $LINE"
    getdeps $LINE
    if [ -f docker/$LINE/Dockerfile ] ; then
        docker build docker/$LINE --tag sql-runner-$LINE
    fi
done <<~
s3
snowflake
azuredwh
redshift
postgres
bigquery
~

while read  LINE ; do
    echo "Adding S3 support for $LINE"
    cp docker/s3/requirements-extra.txt docker/$LINE/requirements-s3.txt
done <<~
snowflake
redshift
postgres
~

#getdeps azuredwh
