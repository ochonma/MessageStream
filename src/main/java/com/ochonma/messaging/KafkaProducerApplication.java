package com.ochonma.messaging;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerApplication implements CommandLineRunner {

    Logger logger = org.apache.logging.log4j.LogManager.getLogger(KafkaProducerApplication.class);
	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}
	
	private Properties registerProducerProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}
	
	void sendMessageToKafka(String topic, String key, String value)throws InterruptedException, ExecutionException
    {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(registerProducerProperties())) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            Future<RecordMetadata> callBack = producer.send(producerRecord);
            RecordMetadata metaData = callBack.get();
            if (logger.isInfoEnabled()) {
                logger.info("message sent to topic {} partition {} offset {}", metaData.topic(), metaData.partition(), metaData.offset());
            }
        }
    }

	@Override
	public void run(String... args)
        {
            try {
                sendMessageToKafka("test","msg1","Its going to be fun");
            }
            catch (InterruptedException e) {
                logger.error("Interruption exception", e);
            }
            catch (ExecutionException e) {
                logger.error("execution exception", e);
            }
            catch (Exception e) {
                logger.error("Error sending message to Kafka", e);
            }
    }
}
