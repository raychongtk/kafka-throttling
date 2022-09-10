package app.kafka;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MessageConsumerThread extends Thread {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final RateLimiter rateLimiter;
    private final String threadName;

    public MessageConsumerThread(String threadName, RateLimiter rateLimiter) {
        this.threadName = threadName;
        this.rateLimiter = rateLimiter;
        kafkaConsumer = new KafkaConsumer<>(KafkaConfig.kafkaConsumerConfig());
        kafkaConsumer.subscribe(List.of(Topics.NOTIFICATION_TOPIC));
    }

    @Override
    public void run() {
        try {
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
                        System.out.printf("threadName = %s, partition = %d, offset = %d, key = %s, value = %s\n", threadName, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                        lastConsumedRecord = consumerRecord;
                    }
                    if (lastConsumedRecord != null) {
                        OffsetAndMetadata lastOffset = new OffsetAndMetadata(lastConsumedRecord.offset() + 1);
                        kafkaConsumer.commitSync(Map.of(partition, lastOffset));
                        kafkaConsumer.seek(partition, lastOffset);
                    }
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
