#
#  Copyright (C) GridGain Systems. All Rights Reserved.
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#

# Extend jre image
FROM openjdk:11-jre-slim
# Set the location of the application
ENV WEB_CONSOLE_HOME /opt/ignite-web-console-server

WORKDIR $WEB_CONSOLE_HOME

# Copy your verticle to the container
COPY ./ignite-web-console-*/*.jar ./
COPY ./ignite-web-console-*/web-console.sh ./
COPY ./ignite-web-console-*/libs ./libs
COPY ./ignite-web-console-*/agent_dists ./agent_dists

EXPOSE 3000

CMD ./web-console.sh
