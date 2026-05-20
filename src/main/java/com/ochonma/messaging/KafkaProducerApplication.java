package com.ochonma.messaging;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}
	
	Properties registerProducerProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}
	
	void sendMessageToKafka(String topic, String key, String value) {
		// create a producer
		// create a record
		// send the record
		// close the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(registerProducerProperties());
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		
		try {
			Future<RecordMetadata> callBack= producer.send(record);
			RecordMetadata metaData=callBack.get();
			System.out.println("message sent to topic "+metaData.topic()+" partition "+metaData.partition()+" offset "+metaData.offset());
		}
		catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			producer.close();
		}
		
		
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		sendMessageToKafka("test","msg1","Its going to be fun");
	}

}
