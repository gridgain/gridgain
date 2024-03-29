#!/bin/sh

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

set -e

pwd="123456"

#
# Create certificate authority with a specified name.
#
# param #1 CA name.
#
function createCa {
    ca_name=$1

    echo
    echo Create a certificate signing request for ${ca_name}.
    openssl req -new -newkey rsa:2048 -nodes -out ${ca_name}.csr -keyout ${ca_name}.key \
        -subj "/emailAddress=${ca_name}@ignite.apache.org/CN=${ca_name}/OU=Dev/O=Ignite/L=SPb/ST=SPb/C=RU"

    echo
    echo Self-sign the CSR for ${ca_name}.
    openssl x509 -trustout -signkey ${ca_name}.key -days 7305 -req -in ${ca_name}.csr -out ${ca_name}.pem

    rm ${ca_name}.csr

    echo
    echo Create auxiliary files for ${ca_name}.
    touch ${ca_name}-index.txt
    echo 01 > ${ca_name}-serial
    echo "
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

[ ca ]
default_ca = ${ca_name}

[ ${ca_name} ]
dir=ca
certificate = \$dir/${ca_name}.pem
database = \$dir/${ca_name}-index.txt
private_key = \$dir/${ca_name}.key
new_certs_dir = \$dir/certs
default_md = sha1
policy = policy_match
serial = \$dir/${ca_name}-serial
default_days = 365

[policy_match]
commonName = supplied" > ${ca_name}.cnf
}

mkdir ca

cd ca

createCa oneca
createCa twoca
createCa threeca

cd ..

# Create four trust stores: trust-one, trust-two, trust-three and trust-both.
# trust-both contains keys of oneca and twoca.

keytool -import -noprompt -file ca/oneca.pem -alias oneca -keypass ${pwd} -storepass ${pwd} -keystore trust-one.jks -storetype JKS
keytool -import -noprompt -file ca/twoca.pem -alias twoca -keypass ${pwd} -storepass ${pwd} -keystore trust-two.jks -storetype JKS
keytool -import -noprompt -file ca/threeca.pem -alias threeca -keypass ${pwd} -storepass ${pwd} -keystore trust-three.jks -storetype JKS

keytool -import -noprompt -file ca/oneca.pem -alias oneca -keypass ${pwd} -storepass ${pwd} -keystore trust-both.jks -storetype JKS
keytool -import -noprompt -file ca/twoca.pem -alias twoca -keypass ${pwd} -storepass ${pwd} -keystore trust-both.jks -storetype JKS
