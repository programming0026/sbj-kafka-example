package com.juniverse.broker.dto;

import org.springframework.web.bind.annotation.*;

/**
 * Cuerpo de la petición para enviar un mensaje.
 *
 * @param content texto del mensaje
 * @param sender quién lo envía
 */
public record MessageRequest(String content, String sender) {}
