package app.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class MessageProducer {
    public void produce() {
        Random random = new Random();
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(KafkaConfig.kafkaProducerConfig())) {
            while (true) {
                NotificationChannel notificationChannel = NotificationChannel.values()[random.nextInt(3)];
                int partition = PartitionMapper.partition(notificationChannel);
                kafkaProducer.send(new ProducerRecord<>(Topics.NOTIFICATION_TOPIC, partition, "key", "testValue"));
            }
        }
    }
}
