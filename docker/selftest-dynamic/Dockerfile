FROM deptdata/sql-runner

RUN apt-get update && apt-get install -y git

RUN mkdir /app
WORKDIR /app

# Create project structure
RUN git init && git remote add origin <REPOSITORY URL USING SSH>
# Populate the git-server-key file using this command on your computer. Review, copy and paste the output to git-server-key
# ssh-keyscan -p 22 <REPOSITORY HOST NAME>
# Add git key fingerprint
COPY git-server-key ~/.ssh/known_hosts

# Generates RSA key pair on the docker image. Otherwise provide a private key using COPY directive
RUN ssh-keygen -t rsa -b 4096 -C "deploy_key" -f /root/.ssh/id_rsa

COPY entrypoint.sh /app/entrypoint.sh

# If a key pair was generated, this outputs the public key.
RUN echo "Add this public key as deployment key to repository:\n" && cat /root/.ssh/id_rsa.pub && echo ""

# Edit this entrypoint to match the config file format you have in your repository
ENTRYPOINT ["/app/entrypoint.sh", "sqlrunner", "--config", "config.py"]
