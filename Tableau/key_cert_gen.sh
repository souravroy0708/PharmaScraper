#! /bin/bash

export HOSTNAME=`hostname -f`
export CERTS_DIR=/home/vagrant/certs

cd $CERTS_DIR;


echo "Generate a private key, and self signed certificate to use as the root CA"
echo "Creating private key for the CA"
openssl genrsa -out private.key 2048
echo "Creating a self signed root CA certificate"
openssl req -x509 -nodes -new -key private.key -days 1000 \
-out ca.crt -subj "/C=US/ST=New York/L=NYC/O=mongodb/CN=bryanCA" 

echo "Creating a certificate signing request (CSR) for mongod"
openssl req -new -nodes -newkey rsa:2048 \
-keyout mongod.key -out mongod.csr \
-subj "/C=US/ST=New York/L=NYC/O=mongodb/CN=$HOSTNAME"

echo "Sign the mongod csr"
openssl x509 -CA ca.crt -CAkey private.key -CAcreateserial \
-req -days 1000 -in mongod.csr -out mongod.crt

echo "Combining the mongod.key and signed certificate into a pem file"
cat mongod.key mongod.crt > mongod.pem

echo "Creating a certificate signing request (CSR) for mongosqld as client"
openssl req -new -nodes -newkey rsa:2048 \
-keyout mongosqld-client.key -out mongosqld-client.csr \
-subj "/C=US/ST=New York/L=NYC/O=mongodb/CN=mongosqld-client"

echo "Sign the mongosqld-client csr"
openssl x509 -CA ca.crt -CAkey private.key -CAserial ca.srl -req -days 1000 -in mongosqld-client.csr -out mongosqld-client.crt

echo "Creating the mongosqld-client pem"
cat mongosqld-client.key mongosqld-client.crt > mongosqld-client.pem

echo "Creating a certificate signing request (CSR) for mongosqld as server"
openssl req -new -nodes -newkey rsa:2048 \
-keyout mongosqld-server.key \
-out mongosqld-server.csr \
-subj "/C=US/ST=New York/L=NYC/O=mongodb/CN=$HOSTNAME"

echo "Sign the mongosqld-server csr"
openssl x509 -CA ca.crt -CAkey private.key -CAserial ca.srl -req -days 1000 -in mongosqld-server.csr -out mongosqld-server.crt

echo "create the mongosqld-server pem"
cat mongosqld-server.key mongosqld-server.crt > mongosqld-server.pem

echo "Create a certificate signing request (CSR) for mysql"
openssl req -new -nodes \
-keyout mysql.key \
-out mysql.csr \
-subj "/C=US/ST=New York/L=NYC/O=mongodb/CN=mysql"

echo "Sign the mongosqld csr"
openssl x509 -CA ca.crt -CAkey private.key -CAserial ca.srl -req -days 1000 -in mysql.csr -out mysql.crt

echo "create the mongosqld pem"
openssl rsa -in mysql.key -out mysql.key
cat mysql.key mysql.crt > mysql.pem


echo "Verify all generated certificates"
openssl verify -CAfile ca.crt mongod.crt;
openssl verify -CAfile ca.crt mongosqld-client.crt;
openssl verify -CAfile ca.crt mongosqld-server.crt;
openssl verify -CAfile ca.crt mysql.crt;
