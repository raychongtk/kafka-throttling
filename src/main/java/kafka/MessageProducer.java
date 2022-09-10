package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class MessageProducer {
    public void produce() {
        Random random = new Random();
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(KafkaConfig.kafkaProducerConfig())) {
            while (true) {
                String key = String.valueOf(random.nextInt(10) + 1);
                kafkaProducer.send(new ProducerRecord<>(Topics.NOTIFICATION_TOPIC, key, "testValue"));
            }
        }
    }
}
