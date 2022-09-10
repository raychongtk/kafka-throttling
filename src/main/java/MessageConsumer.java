import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MessageConsumer {
    public void consume() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 400);
        props.put("max.poll.interval.ms", 10000);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        RateLimiter rateLimiter = RateLimiter.create(500);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("notification"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (rateLimiter.tryAcquire()) consumer.resume(consumer.paused());
                if (records.isEmpty()) continue;

                for (TopicPartition partition : records.partitions()) {
                    ConsumerRecord<String, String> lastRecord = null;
                    for (ConsumerRecord<String, String> record : records.records(partition)) {
                        if (!rateLimiter.tryAcquire()) {
                            System.out.println("throttled");
                            consumer.pause(consumer.assignment());
                            break;
                        }
                        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                        lastRecord = record;
                    }
                    if (lastRecord != null) {
                        OffsetAndMetadata lastOffset = new OffsetAndMetadata(lastRecord.offset() + 1);
                        consumer.commitSync(Map.of(partition, lastOffset));
                        consumer.seek(partition, lastOffset);
                    }
                }
            }
        }
    }
}
