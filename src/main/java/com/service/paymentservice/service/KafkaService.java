package com.service.paymentservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class KafkaService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void sendMessage(String messageType, Object data) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", messageType);
            message.put("data", data);
            message.put("timestamp", getISTTimestamp());
            message.put("service", "payment-service");
            message.put("messageId", UUID.randomUUID().toString());

            kafkaTemplate.send(topicName, message);
            logger.info("Message sent to Kafka topic '{}': {}", topicName, messageType);
        } catch (Exception e) {
            logger.error("Error sending message to Kafka: {}", e.getMessage(), e);
        }
    }

    public Map<String, Object> sendTestMessage() {
        try {
            Map<String, Object> testMessage = new HashMap<>();
            testMessage.put("type", "test");
            testMessage.put("data", "Testing kafka message from payment service");
            testMessage.put("timestamp", getISTTimestamp());
            testMessage.put("service", "payment-service");
            testMessage.put("testId", generateTestId());

            kafkaTemplate.send(topicName, testMessage);
            logger.info("Test message sent to Kafka topic '{}': {}", topicName, testMessage.get("testId"));
            
            return testMessage;
        } catch (Exception e) {
            logger.error("Error sending test message to Kafka: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send test message", e);
        }
    }

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessage(@Payload Map<String, Object> message, 
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        try {
            logger.info("Received message from topic '{}': {}", topic, message.get("type"));
            handlePaymentMessage(message);
        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage(), e);
        }
    }

    private void handlePaymentMessage(Map<String, Object> message) {
        String messageType = (String) message.get("type");
        Object data = message.get("data");
        String service = (String) message.get("service");

        switch (messageType) {
            case "test":
                logger.info("Received test message from {}: {}", service, data);
                break;
            case "order_created":
                logger.info("Order created - processing payment: {}", data);
                // Handle order creation for payment processing
                break;
            case "order_cancelled":
                logger.info("Order cancelled - processing refund: {}", data);
                // Handle order cancellation for refund processing
                break;
            case "payment_required":
                logger.info("Payment required for order: {}", data);
                // Handle payment processing
                break;
            default:
                logger.info("Unknown message type: {}", messageType);
        }
    }

    private String getISTTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "+05:30";
    }

    private String generateTestId() {
        return "test_" + System.currentTimeMillis() % 1000000;
    }
}
