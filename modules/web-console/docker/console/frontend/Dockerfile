#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Extend nginx image
FROM nginx:1-alpine

# Provide default arguments
ARG DEFAULT_DOCKERIZE_VERSION=v0.6.1

ENV DOCKERIZE_VERSION=$DEFAULT_DOCKERIZE_VERSION

RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY ./ignite-web-console-*/frontend /data/www
COPY ./frontend/nginx/nginx.conf /etc/nginx/nginx.conf
COPY ./frontend/nginx/web-console.conf /etc/nginx/web-console.conf

RUN mkdir -p /var/cache/nginx && \
    chgrp -R 0 /var/cache/nginx && \
    chmod -R g=u /var/cache/nginx

USER 10001

EXPOSE 8008
