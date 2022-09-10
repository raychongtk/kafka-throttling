package kafka;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MessageConsumer {
    public void consume() {
        RateLimiter rateLimiter = RateLimiter.create(500);
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(KafkaConfig.kafkaConsumerConfig())) {
            kafkaConsumer.subscribe(List.of(Topics.NOTIFICATION_TOPIC));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
                if (rateLimiter.tryAcquire()) kafkaConsumer.resume(kafkaConsumer.paused());
                if (consumerRecords.isEmpty()) continue;

                for (TopicPartition partition : consumerRecords.partitions()) {
                    ConsumerRecord<String, String> lastConsumedRecord = null;
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords.records(partition)) {
                        if (!rateLimiter.tryAcquire()) {
                            System.out.println("throttled");
                            kafkaConsumer.pause(kafkaConsumer.assignment());
                            break;
                        }
                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                        lastConsumedRecord = consumerRecord;
                    }
                    if (lastConsumedRecord != null) {
                        OffsetAndMetadata lastOffset = new OffsetAndMetadata(lastConsumedRecord.offset() + 1);
                        kafkaConsumer.commitSync(Map.of(partition, lastOffset));
                        kafkaConsumer.seek(partition, lastOffset);
                    }
                }
            }
        }
    }
}
