#!/usr/bin/env bash

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

set -x

# Set Open SSL variables.

# Certificates password.
PASS=123456

# Server.
SERVER_DOMAIN_NAME=localhost
SERVER_EMAIL=support@test.local

# Cleanup.
rm -vf server.*
rm -vf client.*
rm -vf ca.*
rm -vf in*
rm -vf seri*
rm -vf trust*
rm -vf *pem
rm -vf *cnf

# Generate server config.
cat << EOF > server.cnf
[req]
prompt                 = no
distinguished_name     = dn
req_extensions         = req_ext
[ dn ]
countryName            = RU
stateOrProvinceName    = Moscow
localityName           = Moscow
organizationName       = test
commonName             = ${SERVER_DOMAIN_NAME}
organizationalUnitName = IT
emailAddress           = ${SERVER_EMAIL}
[ req_ext ]
subjectAltName         = @alt_names
[ alt_names ]
DNS.1                  = ${SERVER_DOMAIN_NAME}
EOF

# Generate CA config.
cat << EOF > ca.cnf
[ ca ]
default_ca = CertificateAuthority

[ CertificateAuthority ]
certificate = ./ca_generator.pem
database = ./index.txt
private_key = ./ca.key
new_certs_dir = ./
default_md = sha1
policy = policy_match
serial = ./serial
default_days = 1095

[policy_match]
commonName = supplied
EOF

touch index.txt
echo 01 > serial

# Generate CA
openssl req -new -newkey rsa:2048 -nodes -config server.cnf -out ca.csr -keyout ca.key
openssl x509 -trustout -signkey ca.key -req -in ca.csr -out ca_generator.pem
openssl x509 -trustout -signkey ca.key -req -in ca.csr -out ca.crt

keytool -deststorepass ${PASS} -noprompt  -import -file ca_generator.pem -alias CertificateAuthority -keystore trust.jks

# Generate Server certs
keytool -genkey -keyalg RSA -keysize 2048 -alias server -deststorepass ${PASS} -keystore server.jks -noprompt \
 -dname "CN=mqttserver.gridgain.com, OU=ID, O=GridGain, C=US" \
 -storepass ${PASS} \
 -keypass ${PASS}

keytool -deststorepass ${PASS} -certreq -alias server -file server.csr -keystore server.jks
openssl ca -batch -config ca.cnf -out server.pem -infiles server.csr

keytool -deststorepass ${PASS} -import -alias ca -keystore server.jks -file ca_generator.pem -noprompt
keytool -deststorepass ${PASS} -import -alias server -keystore server.jks -file server.pem -noprompt

keytool -importkeystore -srcstoretype JKS -deststoretype PKCS12 -srckeystore server.jks -destkeystore server.p12 -srcstorepass ${PASS} -deststorepass ${PASS} -srcalias server -destalias server -noprompt
openssl pkcs12 -in server.p12 -out server.pem -passin pass:${PASS} -nodes
openssl pkcs12 -in server.p12 -out ca.pem -passin pass:${PASS} -nodes
openssl pkcs12 -in server.p12 -passin pass:${PASS} -nodes -nokeys -out server.crt
openssl pkcs12 -in server.p12 -passin pass:${PASS} -nodes -nocerts -out server.key

# Generate Client certs
# -dname should be different from server
keytool -genkey -keyalg RSA -keysize 2048 -alias client -deststorepass ${PASS} -keystore client.jks -noprompt \
 -dname "CN=clein.gridgain.com, OU=Qs, O=GridGain, C=US" \
 -storepass ${PASS} \
 -keypass ${PASS}

keytool -deststorepass ${PASS} -certreq -alias client -file client.csr -keystore client.jks
openssl ca -batch -config ca.cnf -out client.pem -infiles client.csr

keytool -deststorepass ${PASS} -import -alias ca -keystore client.jks -file ca_generator.pem -noprompt
keytool -deststorepass ${PASS} -import -alias client -keystore client.jks -file client.pem -noprompt

keytool -importkeystore -srcstoretype JKS -deststoretype PKCS12 -srckeystore client.jks -destkeystore client.p12 -srcstorepass ${PASS} -deststorepass ${PASS} -srcalias client -destalias client -noprompt
openssl pkcs12 -in client.p12 -out client.pem -passin pass:${PASS} -nodes
openssl pkcs12 -in client.p12 -passin pass:${PASS} -nodes -nokeys -out client.crt
openssl pkcs12 -in client.p12 -passin pass:${PASS} -nodes -nocerts -out client.key
