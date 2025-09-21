package com.service.paymentservice.controller;

import com.service.paymentservice.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    private KafkaService kafkaService;

    @GetMapping("/test")
    public ResponseEntity<Map<String, Object>> testKafka() {
        try {
            logger.info("Testing Kafka connection...");
            Map<String, Object> result = kafkaService.sendTestMessage();
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Test message sent to Kafka successfully",
                "data", result
            ));
        } catch (Exception e) {
            logger.error("Error testing Kafka connection: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", "Failed to send test message to Kafka",
                "details", e.getMessage()
            ));
        }
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> sendMessage(@RequestBody Map<String, Object> request) {
        try {
            String messageType = (String) request.get("type");
            Object data = request.get("data");
            
            if (messageType == null || messageType.trim().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "Message type is required"
                ));
            }
            
            kafkaService.sendMessage(messageType, data);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Message sent to Kafka successfully",
                "type", messageType
            ));
        } catch (Exception e) {
            logger.error("Error sending message to Kafka: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", "Failed to send message to Kafka",
                "details", e.getMessage()
            ));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        return ResponseEntity.ok(Map.of(
            "status", "healthy",
            "service", "payment-service",
            "kafka", "connected"
        ));
    }
}
