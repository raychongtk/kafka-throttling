package app.kafka;

public class MessageConsumer {
    public void run(int numberOfThread) {
        MessageRateLimiter messageRateLimiter = new MessageRateLimiter();
        for (int i = 0; i < numberOfThread; i++) {
            new MessageConsumerThread("thread-" + i, messageRateLimiter.getRateLimiter()).start();
        }
    }
}
