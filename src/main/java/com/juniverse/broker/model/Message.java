package com.juniverse.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/** Mensaje que viaja por Kafka (se serializa como JSON). */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private String id;
    private String content;
    private String sender;
    private LocalDateTime timestamp;
}
