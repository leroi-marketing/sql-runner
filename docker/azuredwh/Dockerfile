FROM python:3.8-slim

RUN apt-get update 
RUN apt-get install -y curl apt-transport-https gnupg2 \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools
RUN apt-get install -y git 
RUN apt-get install -y python3-dev gcc build-essential libpq-dev unixodbc-dev -y
RUN curl -sS https://aka.ms/InstallAzureCli | bash 

COPY requirements.txt requirements-extra.txt /tmp/
RUN pip install -r /tmp/requirements.txt 
RUN pip install -r /tmp/requirements-extra.txt 

RUN pip install sql-runner

ENTRYPOINT ["sqlrunner"]
