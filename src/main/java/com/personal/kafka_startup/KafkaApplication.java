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
	private static final String TOPIC_A = "TopicA";
	private static final String TOPIC_B = "TopicB";

	private static final String[] TOPICS = {TOPIC_A, TOPIC_B};

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	/**
	 * This is our publisher tool
	 * @param message
	 */
	public static void sendMessage(String message, String topic, KafkaTemplate<String, String> producer) {
		CompletableFuture<SendResult<String, String>> future = producer.send(topic, message);
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
	@KafkaListener(topics = TOPIC_A, groupId = "group3")
	public void listenGroupFoo(String message) {
		System.out.println("[SIMPLE] Received Message in group foo: "+message);
	}

	@KafkaListener(topics = {"TopicA", "TopicB"}, groupId = "group2")
	public void listenGroupFooMultiTopic(String message) {
		System.out.println("[MULTI] Received Message from multiTopic: "+message);
	}

	@KafkaListener(
			topicPartitions = @TopicPartition(topic = TOPIC_A,
					partitionOffsets = {
						@PartitionOffset(partition = "0", initialOffset = "0"),
						@PartitionOffset(partition = "1", initialOffset = "0"),
					}
			),
			groupId = "group1"
	)
	public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition){
		System.out.println("[OFFSET] Received Message: "+message+" from partition: "+partition);
	}

	@PostConstruct
	public void init() {
		sendMessage("Message1", TOPIC_A, kafkaTemplate);
		sendMessage("Message3", TOPIC_A, kafkaTemplate);
		sendMessage("Message2", TOPIC_A, kafkaTemplate);
		sendMessage("Message4", TOPIC_A, kafkaTemplate);
		sendMessage("Message5", TOPIC_A, kafkaTemplate);
		sendMessage("Message6", TOPIC_A, kafkaTemplate);
		sendMessage("Message7", TOPIC_A, kafkaTemplate);
		sendMessage("Message8", TOPIC_A, kafkaTemplate);
		sendMessage("Message9", TOPIC_A, kafkaTemplate);
		sendMessage("Message10", TOPIC_A, kafkaTemplate);
		sendMessage("Message11", TOPIC_A, kafkaTemplate);
		sendMessage("Message12", TOPIC_A, kafkaTemplate);

		sendMessage("BMessage1", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage3", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage2", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage4", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage5", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage6", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage7", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage8", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage9", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage10", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage11", TOPIC_B, kafkaTemplate);
		sendMessage("BMessage12", TOPIC_B, kafkaTemplate);
	}
}
