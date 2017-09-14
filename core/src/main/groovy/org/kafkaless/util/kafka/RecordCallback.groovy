package org.kafkaless.util.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.utils.Bytes

interface RecordCallback {

    void onRecord(ConsumerRecord<String, Bytes> consumerRecord)

}