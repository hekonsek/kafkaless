# Kafkaless - functions over Kafka data streams

[![Version](https://img.shields.io/badge/kafkaless-0.4-blue.svg)](https://github.com/kafkaless/kafkaless/releases)
[![Build](https://api.travis-ci.org/kafkaless/kafkaless.svg)](https://travis-ci.org/kafkaless/kafkaless/)

Kafkaless brings serverless function processing model to Apache Kafka data streams.

## License

Kafkaless is distributed under Apache 2.0 license.

## Architecture

Kafkaless architecture consists of the following elements:
- Kafka cluster
- Functions connected to Kafka cluster

### Functions

Function is a small application connected to Kafka cluster. Function consumes event from certain topic, processes that event and sends 
processed event to the another topic. 

You can implement function using any programming language you want, but the easiest approach is to
use SDK library we provide. For now we support only Java SDK.