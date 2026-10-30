package com.juniverse.broker.producer;

import com.juniverse.broker.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Productor: publica mensajes en el topic. */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topicName;

    /**
     * Crea un mensaje con identificador y fecha y lo publica.
     * @param content texto del mensaje
     * @param sender remitente
     */
    public void sendMessage(String content, String sender) {
        Message message = Message.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .sender(sender)
                .timestamp(LocalDateTime.now())
                .build();

        sendMessage(message);
    }

    /**
     * Publica el mensaje de forma asíncrona (usa su id como clave, para que los mensajes con el mismo id vayan a la misma partición).
     * @param message mensaje a publicar
     */
    public void sendMessage(Message message) {
        log.info("Sending message: {}", message);

        CompletableFuture<SendResult<String, Message>> future =
                kafkaTemplate.send(topicName, message.getId(), message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Message sent successfully to topic: {}, partition: {}, offset: {}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message: {}", ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publica el mensaje y devuelve el resultado como {@code CompletableFuture}, para que quien llama pueda esperarlo (por ejemplo con {@code join()}).
     * @param message mensaje a publicar
     * @return futuro con el resultado del envío
     */
    public CompletableFuture<SendResult<String, Message>> sendMessageSync(Message message) {
        log.info("Sending message synchronously: {}", message);
        return kafkaTemplate.send(topicName, message.getId(), message);
    }
}
