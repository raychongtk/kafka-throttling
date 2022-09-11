package app.kafka;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MessageConsumerThread extends Thread {
    private final static Logger logger = LoggerFactory.getLogger(MessageConsumerThread.class);
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
                kafkaConsumer.resume(kafkaConsumer.paused());
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
                if (consumerRecords.isEmpty()) continue;

                consume(consumerRecords);
            }
        } catch (Throwable e) {
            logger.error("failed to pull messages", e);
        } finally {
            kafkaConsumer.close();
        }
    }

    private void consume(ConsumerRecords<String, String> consumerRecords) {
        for (TopicPartition partition : consumerRecords.partitions()) {
            ConsumerRecord<String, String> lastConsumedRecord = null;
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords.records(partition)) {
                if (messageThrottled()) break;
                process(consumerRecord);
                lastConsumedRecord = consumerRecord;
            }
            if (lastConsumedRecord != null) commit(partition, lastConsumedRecord);
        }
    }

    private boolean messageThrottled() {
        if (!rateLimiter.tryAcquire()) {
            logger.warn("message throttled");
            kafkaConsumer.pause(kafkaConsumer.assignment());
            return true;
        }
        return false;
    }

    private void process(ConsumerRecord<String, String> consumerRecord) {
        logger.info("threadName={}, partition={}, offset={}, key={}, value={}", threadName, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
    }

    private void commit(TopicPartition partition, ConsumerRecord<String, String> lastConsumedRecord) {
        OffsetAndMetadata lastOffset = new OffsetAndMetadata(lastConsumedRecord.offset() + 1);
        kafkaConsumer.commitSync(Map.of(partition, lastOffset));
        kafkaConsumer.seek(partition, lastOffset);
    }
}
