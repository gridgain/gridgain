FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    default-jdk \
    libssl1.0-dev \
    unixodbc-dev \
    libboost-all-dev \
    libtool \
    automake \
    autoconf

WORKDIR /usr/src/ignite_cpp

COPY . .

RUN libtoolize && \
    aclocal && \
    autoheader && \
    automake --add-missing && \
    autoreconf && \
    ./configure --disable-core --disable-thin-client --disable-node --disable-tests --enable-odbc && \
    make -j8 && \
    make install

WORKDIR /usr/src/ignite_cpp/odbc/install

RUN odbcinst -i -d -f ./ignite-odbc-install.ini -v

WORKDIR /usr/src/ignite_cpp/benchmarks

RUN libtoolize && \
    aclocal && \
    autoheader && \
    automake --add-missing && \
    autoreconf && \
    ./configure && \
    make -j8

ENV CONNECTION_STRING="DRIVER={Apache Ignite};ADDRESS=127.0.0.1"
ENV THREAD_CNT=64
ENV WARMUP=0
ENV DURATION=120
ENV CACHE_NAME=PUBLIC
ENV CACHE_RANGE=0-10000

WORKDIR /usr/src/ignite_cpp/benchmarks/odbc-sql

ENTRYPOINT ["./ignite-odbc-sql-benchmark"]
