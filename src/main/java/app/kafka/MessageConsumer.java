package app.kafka;

public class MessageConsumer {
    public void run() {
        MessageRateLimiter messageRateLimiter = new MessageRateLimiter();
        new MessageConsumerThread("thread-sms", messageRateLimiter.getSmsRateLimiter(), PartitionMapper.partitions(NotificationChannel.SMS)).start();
        new MessageConsumerThread("thread-email", messageRateLimiter.getEmailRateLimiter(), PartitionMapper.partitions(NotificationChannel.EMAIL)).start();
        new MessageConsumerThread("thread-inbox", messageRateLimiter.getInboxRateLimiter(), PartitionMapper.partitions(NotificationChannel.INBOX)).start();
    }
}
