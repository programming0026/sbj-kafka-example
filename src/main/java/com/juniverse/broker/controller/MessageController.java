package com.juniverse.broker.controller;

import com.juniverse.broker.dto.MessageRequest;
import com.juniverse.broker.model.Message;
import com.juniverse.broker.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/** API REST para enviar mensajes a Kafka. */
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    /**
     * Envía un mensaje completo.
     * @param request contenido y remitente
     * @return confirmación con el identificador del mensaje, o 400 si faltan datos
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> sendMessage(@RequestBody MessageRequest request) {
        if (request == null || isBlank(request.content()) || isBlank(request.sender())) {
            return ResponseEntity.badRequest().body(Map.of("error", "content y sender son obligatorios"));
        }
        Message message = Message.builder()
                .id(UUID.randomUUID().toString())
                .content(request.content())
                .sender(request.sender())
                .timestamp(LocalDateTime.now())
                .build();

        messageProducer.sendMessage(message);

        return ResponseEntity.ok(Map.of(
                "status", "Message sent",
                "messageId", message.getId()
        ));
    }

    /**
     * Envía un mensaje indicando contenido y remitente como parámetros de la URL.
     * @param content texto del mensaje
     * @param sender remitente
     * @return confirmación, o 400 si faltan datos
     */
    @PostMapping("/simple")
    public ResponseEntity<Map<String, String>> sendSimpleMessage(
            @RequestParam String content,
            @RequestParam String sender) {

        if (isBlank(content) || isBlank(sender)) {
            return ResponseEntity.badRequest().body(Map.of("error", "content y sender son obligatorios"));
        }
        messageProducer.sendMessage(content, sender);
        return ResponseEntity.ok(Map.of(
                "status", "Message sent",
                "content", content,
                "sender", sender
        ));
    }

    /** Indica si el texto es nulo o está en blanco. */
    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
