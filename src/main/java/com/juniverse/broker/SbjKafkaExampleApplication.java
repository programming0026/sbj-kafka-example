package com.juniverse.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Aplicación de ejemplo: productor y consumidor de Apache Kafka con Spring Boot. */
@SpringBootApplication
public class SbjKafkaExampleApplication {

	/**
	 * Arranca la aplicación.
	 * @param args argumentos de la línea de comandos
	 */
	public static void main(String[] args) {
		SpringApplication.run(SbjKafkaExampleApplication.class, args);
	}

}
