package com.personal.kafka_startup;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    /**
     * Setups the configuration for the kafka topic
     *      * adds in the bootstrap server
     */
    @Bean
    public KafkaAdmin kafkaAdmin(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    /**
     * You can create your topics this way
     *
     */
    @Bean
    public NewTopic topic1() {
        return new NewTopic("TopicA", 1, (short) 1);
    }

    /**
     * Create another topic
     * @return
     */
    @Bean
    public NewTopic topic2() {
        return new NewTopic("TopicB", 1, (short) 1);
    }
}
