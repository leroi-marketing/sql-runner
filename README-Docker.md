# Docker image definitions

In `docker/`, there is a directory for each DWH provider, with everything that's needed to build it.

These are all **base images**, except `selftest`. In order to make it work, a new image which handles actual SQL files, and sqlrunner configuration must be built from it. `selftest` is an example of such an implementation

## Building and pushing base images

Pushing a base image allows other computers to build derived images out of it. In order to prepare to build, run the `pin-dependencies.sh` script, and take care of any version conflicts you encounter. This will generate or update multiple `requirements*.txt` files for every Docker image.

To build the `snowflake` version of sqlrunner:

```sh
docker build docker/snowflake --tag sqlrunner-snowflake:latest
docker tag sqlrunner-snowflake:latest <target-repository>/sqlrunner-snowflake:latest
docker push <target-repository>/sqlrunner-snowflake:latest
```

## Building and pushing specific image

There are 2 options for the specific images: Static and Dynamic. Whichever is built, make sure to base it on the appropriate base image using the `FROM` clause.

### Static
A static image receives all the information it needs (sql files) at `build` time. It doesn't change functionally during runtime. It has to be re-built every time sql files change. This is best suited for CI/CD tools. Without automation of this process however it becomes difficult to manage business logic.

Use the `selftest-static` image as an example. This is all you need to put together with your SQL code. Docker build path (in this case `.`) should be the parent directory for all other resources used to build the container. In this case, the `selftest/` directory is located in the root path of the repository, which is why it's the base path for docker build. `-f` directory is optional and points to where `Dockerfile` is located.

```sh
docker build -f docker/selftest-static/Dockerfile . --tag selftest-sqlrunner-static:latest
docker tag selftest-sqlrunner-static:latest <target-repository>/selftest-sqlrunner-static:latest
docker push <target-repository>/selftest-sqlrunner-static:latest
```

### Dynamic
A dynamic image configures each container to retrieve sources (sql files) from another place. The image is built only when new versions of sqlrunner are released or config changes are made that require the image to rebuild. This makes it easy to focus only on the business logic in SQL when you don't have CI/CD tools. But building the image is more involved. Make sure to go through the `Dockerfile` and adapt it to your situation.

```sh
docker build docker/selftest-dynamic --tag selftest-sqlrunner-dynamic:latest
docker tag selftest-sqlrunner-dynamic:latest <target-repository>/selftest-sqlrunner-dynamic:latest
docker push <target-repository>/selftest-sqlrunner-dynamic:latest
```

This requires additional set up for the git repository used, so that the application in the docker container can pull from the git repository.
