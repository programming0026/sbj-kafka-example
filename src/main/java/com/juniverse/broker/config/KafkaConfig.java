package com.juniverse.broker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Configuración de Kafka: crea el topic al arrancar. */
@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic}")
    private String topicName;

    /**
     * Topic de mensajes con 3 particiones y 1 réplica.
     * @return definición del topic
     */
    @Bean
    public NewTopic messagesTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
