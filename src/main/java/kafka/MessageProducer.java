package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class MessageProducer {
    public void produce() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.kafkaProducerConfig())) {
            while (true) {
                Random random = new Random();
                int value = random.nextInt(10) + 1;
                producer.send(new ProducerRecord<>(Topics.NOTIFICATION_TOPIC, value + "", "abc"));
            }
        }
    }
}
