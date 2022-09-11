package app.kafka;

public class MessageConsumer {
    public void run(int numberOfThread) {
        for (int i = 0; i < numberOfThread; i++) {
            new MessageConsumerThread("thread-" + i).start();
        }
    }
}
