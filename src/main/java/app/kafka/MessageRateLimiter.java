package app.kafka;

import com.google.common.util.concurrent.RateLimiter;

public class MessageRateLimiter {
    private RateLimiter smsRateLimiter;
    private RateLimiter emailRateLimiter;
    private RateLimiter inboxRateLimiter;

    private RateLimiter getSmsRateLimiter() {
        if (smsRateLimiter == null) {
            smsRateLimiter = RateLimiter.create(3000);
        }
        return smsRateLimiter;
    }

    private RateLimiter getInboxRateLimiter() {
        if (inboxRateLimiter == null) {
            inboxRateLimiter = RateLimiter.create(500);
        }
        return inboxRateLimiter;
    }

    private RateLimiter getEmailRateLimiter() {
        if (emailRateLimiter == null) {
            emailRateLimiter = RateLimiter.create(2000);
        }
        return emailRateLimiter;
    }

    public RateLimiter getRateLimit(NotificationChannel notificationChannel) {
        if (notificationChannel == NotificationChannel.SMS) return getSmsRateLimiter();
        if (notificationChannel == NotificationChannel.EMAIL) return getEmailRateLimiter();
        if (notificationChannel == NotificationChannel.INBOX) return getInboxRateLimiter();
        throw new IllegalArgumentException("invalid notification channel");
    }
}
