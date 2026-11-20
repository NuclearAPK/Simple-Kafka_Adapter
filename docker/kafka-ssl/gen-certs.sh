#!/bin/sh
# Test certificates for docker/kafka-ssl (specs/ssl-fail-fast.md).
# The broker certificate has CN=kafka and NO subjectAltName, so a client connecting to
# 127.0.0.1 fails host name verification exactly like the kafka.internal stand.
# Run from this directory (WSL or Git Bash with openssl): sh gen-certs.sh
set -e
mkdir -p certs
cd certs

openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=TestCA" \
	-keyout ca.key -out ca-cert.pem

openssl req -newkey rsa:2048 -nodes -subj "/CN=kafka" -keyout broker.key -out broker.csr
openssl x509 -req -in broker.csr -CA ca-cert.pem -CAkey ca.key -CAcreateserial -days 365 -out broker.crt
# Kafka PEM keystore: private key + certificate chain in one file
cat broker.key broker.crt ca-cert.pem > broker-keystore.pem

openssl req -newkey rsa:2048 -nodes -subj "/CN=client" -keyout client.key -out client.csr
openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca.key -CAcreateserial -days 365 -out client.crt

# A CA that signed nothing: for the "wrong CA" scenario
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=WrongCA" \
	-keyout wrong-ca.key -out wrong-ca.pem

# Names expected by tests/ssl-fail-fast
cp ca-cert.pem ca-cert.txt
rm -f *.csr *.srl
chmod 644 *
