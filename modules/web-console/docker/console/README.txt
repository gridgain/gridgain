GridGain Web Console Docker module
=====================================
GridGain Web Console Docker module provides Dockerfile and accompanying files
for building docker image of Web Console.


GridGain Web Console Docker Image Build Instructions
===========
1) Assembly GridGain Web Console binary archive:

        mvn clean package \
            -Plgpl,web-console,release \
            -pl :ignite-web-console -am \
            -DskipTests -DskipClientDocs -Dmaven.javadoc.skip=true

2) Go to GridGain Web Console Docker module directory and copy GridGain Web binary archive

    cd modules/web-console/docker/console
    cp -rfv ../../target/ignite-web-console-*.zip ./

3) Unpack GridGain Web Console binary archive

    unzip ignite-web-console-*.zip

4) Build backend docker image

    docker build . -f backend/Dockerfile -t gridgain/web-console-backend[:<version>]

    Prepared image will be available in local docker registry (can be seen issuing `docker images` command)

5) Build frontend docker image

    docker build . -f frontend/Dockerfile -t gridgain/web-console-frontend[:<version>]

6) Clean up

    rm -rf ignite-web-console-*
    