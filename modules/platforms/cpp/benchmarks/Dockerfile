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

WORKDIR /usr/src/ignite_cpp/benchmarks

RUN libtoolize && \
    aclocal && \
    autoheader && \
    automake --add-missing && \
    autoreconf && \
    ./configure --disable-core --disable-thin-client --disable-node --disable-tests --enable-odbc && \
    make -j8

