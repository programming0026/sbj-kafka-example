package com.juniverse.broker.consumer;

import com.juniverse.broker.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/** Consumidor: escucha el topic y registra cada mensaje recibido. */
@Slf4j
@Service
public class MessageConsumer {

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    /**
     * Recibe un mensaje del topic.
     * @param message mensaje deserializado desde JSON
     * @param topic topic de origen
     * @param partition partición de origen
     * @param offset posición del mensaje en la partición
     */
    public void consume(
            @Payload Message message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("============================================");
        log.info("Message received!");
        log.info("Topic: {}", topic);
        log.info("Partition: {}", partition);
        log.info("Offset: {}", offset);
        log.info("Message ID: {}", message.getId());
        log.info("Content: {}", message.getContent());
        log.info("Sender: {}", message.getSender());
        log.info("Timestamp: {}", message.getTimestamp());
        log.info("============================================");

        processMessage(message);
    }

    /**
     * Aquí iría la lógica de negocio que procesa el mensaje.
     * @param message mensaje recibido
     */
    private void processMessage(Message message) {
        // Business logic to process the message
        log.info("Processing message from sender: {}", message.getSender());
    }
}
