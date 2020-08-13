FROM deptdata/sql-runner

RUN mkdir /app
WORKDIR /app
COPY selftest sql
COPY docker/selftest/config.py config.py

ENTRYPOINT ["sqlrunner", "--config", "/app/config.py"]
CMD ["--cold-run", "--execute", "create_testdata", "selftest"]
