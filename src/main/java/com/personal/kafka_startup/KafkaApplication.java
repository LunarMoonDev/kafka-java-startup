package com.personal.kafka_startup;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class KafkaApplication {
	@Autowired
	public KafkaTemplate<String, String> kafkaTemplate;
	private static final String TOPIC = "TopicA";

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	/**
	 * This is our publisher tool
	 * @param message
	 */
	public static void sendMessage(String message, String topic, KafkaTemplate<String, String> producer) {
		CompletableFuture<SendResult<String, String>> future = producer.send(TOPIC, message);
		future.whenComplete((result, ex) -> {
			if (ex == null) {
				System.out.println("Sent message=["+message+"] with offset=["+result.getRecordMetadata().offset()+"]");
			} else {
				System.out.println("Unable to send message=["+message+"] due to : "+ex.getMessage());
			}
		});
	}

	/**
	 * Consumer that listens to the topic and processes it
	 * @param message
	 */
	@KafkaListener(topics = TOPIC, groupId = "groupId")
	public void listenGroupFoo(String message) {
		System.out.println("Received Message in group foo: "+message);
	}

	@PostConstruct
	public void init() {
		sendMessage("Hello World!", TOPIC, kafkaTemplate);
	}
}
