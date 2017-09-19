FROM openjdk:8u141-jre

MAINTAINER hekonsek@gmail.com

RUN curl http://ftp.ps.pl/pub/apache/kafka/0.11.0.1/kafka_2.12-0.11.0.1.tgz  | tar xvz
RUN mv /kafka_2.12-0.11.0.1 /usr/kafka_2.12-0.11.0.1

ADD kafka.sh /usr/kafka_2.12-0.11.0.1/
RUN chmod +x /usr/kafka_2.12-0.11.0.1/kafka.sh

VOLUME ["/kafka"]

EXPOSE 9092

ENTRYPOINT ["/usr/kafka_2.12-0.11.0.1/kafka.sh"]