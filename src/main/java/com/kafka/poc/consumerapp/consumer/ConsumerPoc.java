package com.kafka.poc.consumerapp.consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;


@Component
public class ConsumerPoc {
	@PostConstruct
	public void init(){
		   Properties consumerConfig = new Properties();
	       consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "apache-kafka:9092");
	       consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group2");
	       consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	       consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	       consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	       KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
	       TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
	       consumer.subscribe(Collections.singletonList("test"), rebalanceListener);

	       while (true) {
	           ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
	           for (ConsumerRecord<byte[], byte[]> record : records) {
	               System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
	           }

	           consumer.commitSync();
	       }
	}
	
	private static class  TestConsumerRebalanceListener implements ConsumerRebalanceListener {
	       @Override
	       public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
	           System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
	       }

	       @Override
	       public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
	           System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
	       }
	   }

}
