package app;

import app.kafka.MessageConsumer;
import app.kafka.MessageProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.submit(() -> new MessageProducer().produce());

        new MessageConsumer().run(10);
    }
}
