package app.kafka;

import java.util.Properties;

public class KafkaConfig {
    private static final String HOST = "localhost:29092";

    public static Properties kafkaConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", HOST);
        properties.put("group.id", "notification-group");
        properties.put("enable.auto.commit", "false");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", 400);
        properties.put("max.poll.interval.ms", 10000);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static Properties kafkaProducerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", HOST);
        properties.put("acks", "all");
        properties.put("retries", Integer.MAX_VALUE);
        properties.put("retry.backoff.ms", 1000);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 12000);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}
