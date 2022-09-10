package app.kafka;

import com.google.common.util.concurrent.RateLimiter;

public class MessageRateLimiter {
    private RateLimiter rateLimiter;

    public RateLimiter getRateLimiter() {
        if (rateLimiter == null) {
            rateLimiter = RateLimiter.create(500);
        }
        return rateLimiter;
    }
}
