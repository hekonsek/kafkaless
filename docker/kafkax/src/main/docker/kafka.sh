#!/usr/bin/env bash

if [ -z "${ZOOKEEPER_SERVICE_HOST}" ]; then
    ZOOKEEPER_SERVICE_HOST='localhost'
fi

sed -i "/^zookeeper.connect=/s/=.*/=${ZOOKEEPER_SERVICE_HOST}/" /usr/kafka_2.12-0.11.0.1/config/server.properties
sed -i "/^log.dirs=/s|=.*|=/kafka|" /usr/kafka_2.12-0.11.0.1/config/server.properties
echo >> /usr/kafka_2.12-0.11.0.1/config/server.properties
echo "advertised.host.name=${KAFKA_SERVICE_HOST}" >> /usr/kafka_2.12-0.11.0.1/config/server.properties
echo "advertised.port=9092" >> /usr/kafka_2.12-0.11.0.1/config/server.properties

/usr/kafka_2.12-0.11.0.1/bin/kafka-server-start.sh /usr/kafka_2.12-0.11.0.1/config/server.properties