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

function createCa {
    ca_name=$1

    openssl req -new -newkey rsa:2048 -nodes -out ${ca_name}.csr -keyout ${ca_name}.key \
        -subj "/emailAddress=${ca_name}@ignite.apache.org/CN=${ca_name}/OU=Dev/O=Ignite/L=SPb/ST=SPb/C=RU"

    openssl x509 -trustout -signkey ${ca_name}.key -days 7305 -req -in ${ca_name}.csr -out ${ca_name}.pem

    rm ${ca_name}.csr

    touch ${ca_name}-index.txt
    echo 01 > ${ca_name}-serial
    echo "[ ca ]
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

#
# Create artifacts for specified name: key pair-> cert request -> ca-signed certificate.
# Save private key and CA-signed certificate into key storages: PEM, JKS, PFX (PKCS12).
#
# param $1 Artifact name.
# param $2 Name of a certificate authority.
# param $3 Password for all keys and storages.
#
function createStore {
	artifact=$1
	ca_name=$2
	expired=$3

	if [[ "$expired" = true ]]; then
        startdate=`date -d '2 days ago' '+%y%m%d%H%M%SZ'`
        enddate=`date -d 'yesterday' '+%y%m%d%H%M%SZ'`
    else
        startdate=`date -d 'today 00:00:00' '+%y%m%d%H%M%SZ'`
        enddate=`date -d 'today + 7305 days' '+%y%m%d%H%M%SZ'`
    fi

	ca_cert=ca/${ca_name}.pem

	echo
	echo Clean up all old artifacts: ${artifact}.*
	rm -f ${artifact}.*

	echo
	echo Generate a certificate and private key pair for ${artifact}.
	keytool -genkey -keyalg RSA -keysize 1024 \
	        -dname "emailAddress=${artifact}@ignite.apache.org, CN=${artifact}, OU=Dev, O=Ignite, L=SPb, ST=SPb, C=RU" \
	        -alias ${artifact} -keypass ${pwd} -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo Create a certificate signing request for ${artifact}.
	keytool -certreq -alias ${artifact} -file ${artifact}.csr -keypass ${pwd} -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo "Sign the CSR using ${ca_name}."
	openssl ca -config ca/${ca_name}.cnf \
	        -startdate ${startdate} -enddate ${enddate} \
	        -batch -out ${artifact}.pem -infiles ${artifact}.csr

	echo
	echo Convert to PEM format.
	openssl x509 -in ${artifact}.pem -out ${artifact}.pem -outform PEM

	echo
	echo Concatenate the CA certificate file and ${artifact}.pem certificate file into certificates chain.
	cat ${artifact}.pem ${ca_cert} > ${artifact}.chain

	echo
	echo Update the keystore, ${artifact}.jks, by importing the CA certificate.
	keytool -import -alias ${ca_name} -file ${ca_cert} -keypass ${pwd} -noprompt -trustcacerts -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo Update the keystore, ${artifact}.jks, by importing the full certificate chain for the ${artifact}.
	keytool -import -alias ${artifact} -file ${artifact}.chain -keypass ${pwd} -noprompt -trustcacerts -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo Generate PKCS12 storage for the private key and certificate chain.
	keytool -importkeystore \
		-srcstoretype JKS -deststoretype PKCS12 \
		-srckeystore ${artifact}.jks -destkeystore ${artifact}.pfx \
		-srcstorepass ${pwd} -deststorepass ${pwd} \
		-srcalias ${artifact} -destalias ${artifact} \
		-srckeypass ${pwd} -destkeypass ${pwd} \
		-noprompt

	echo
	echo Generate PEM storage for the private key and certificate chain.
	openssl pkcs12 \
		-in ${artifact}.pfx -out ${artifact}.pem \
		-passin pass:${pwd} -passout pass:${pwd}

	rm -f ${artifact}.chain ${artifact}.csr ${artifact}.pfx ${artifact}.pem
}

mkdir -p ca/certs

cd ca

createCa oneca
createCa twoca
createCa threeca

cd ..

keytool -import -noprompt -file ca/oneca.pem -alias oneca -keypass ${pwd} -storepass ${pwd} -keystore trust-one.jks
keytool -import -noprompt -file ca/twoca.pem -alias twoca -keypass ${pwd} -storepass ${pwd} -keystore trust-two.jks
keytool -import -noprompt -file ca/threeca.pem -alias threeca -keypass ${pwd} -storepass ${pwd} -keystore trust-three.jks

keytool -import -noprompt -file ca/oneca.pem -alias oneca -keypass ${pwd} -storepass ${pwd} -keystore trust-both.jks
keytool -import -noprompt -file ca/twoca.pem -alias twoca -keypass ${pwd} -storepass ${pwd} -keystore trust-both.jks

createStore cluster oneca
createStore thinClient twoca
createStore thinServer twoca
createStore connectorClient threeca
createStore connectorServer threeca

createStore node01 oneca
createStore node02 twoca
createStore node03 twoca
createStore node02old twoca true

rm -rf ca
