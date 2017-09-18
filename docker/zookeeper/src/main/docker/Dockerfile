FROM openjdk:8u141-jre

MAINTAINER hekonsek@gmail.com

RUN curl http://ftp.ps.pl/pub/apache/kafka/0.11.0.1/kafka_2.12-0.11.0.1.tgz | tar xvz

VOLUME ["/tmp/zookeeper"]

EXPOSE 2181

ENTRYPOINT ["/kafka_2.12-0.11.0.0/bin/zookeeper-server-start.sh", "/kafka_2.12-0.11.0.0/config/zookeeper.properties"]