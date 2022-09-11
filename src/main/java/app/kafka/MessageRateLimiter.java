package app.kafka;

import com.google.common.util.concurrent.RateLimiter;

public class MessageRateLimiter {
    private RateLimiter smsRateLimiter;
    private RateLimiter emailRateLimiter;
    private RateLimiter inboxRateLimiter;

    public RateLimiter getSmsRateLimiter() {
        if (smsRateLimiter == null) {
            smsRateLimiter = RateLimiter.create(3000);
        }
        return smsRateLimiter;
    }

    public RateLimiter getInboxRateLimiter() {
        if (inboxRateLimiter == null) {
            inboxRateLimiter = RateLimiter.create(500);
        }
        return inboxRateLimiter;
    }

    public RateLimiter getEmailRateLimiter() {
        if (emailRateLimiter == null) {
            emailRateLimiter = RateLimiter.create(2000);
        }
        return emailRateLimiter;
    }
}
