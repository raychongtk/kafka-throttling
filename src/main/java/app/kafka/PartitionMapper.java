package app.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class PartitionMapper {
    private static final Map<NotificationChannel, List<Integer>> PARTITION_MAPPING = partitionMapping();
    private static final Map<Integer, NotificationChannel> NOTIFICATION_CHANNEL_MAPPING = notificationMapping();

    public static int partition(NotificationChannel notificationChannel) {
        Random random = new Random();
        List<Integer> partitions = PARTITION_MAPPING.get(notificationChannel);
        return partitions.get(random.nextInt(partitions.size()));
    }

    public static List<Integer> partitions(NotificationChannel notificationChannel) {
        return PARTITION_MAPPING.get(notificationChannel);
    }

    public static NotificationChannel notificationChannel(int partition) {
        return NOTIFICATION_CHANNEL_MAPPING.get(partition);
    }

    private static Map<NotificationChannel, List<Integer>> partitionMapping() {
        Map<NotificationChannel, List<Integer>> partitionMapping = new HashMap<>();
        partitionMapping.put(NotificationChannel.SMS, List.of(0, 1, 2, 3));
        partitionMapping.put(NotificationChannel.EMAIL, List.of(4, 5, 6));
        partitionMapping.put(NotificationChannel.INBOX, List.of(7, 8, 9));
        return partitionMapping;
    }

    private static Map<Integer, NotificationChannel> notificationMapping() {
        Map<Integer, NotificationChannel> notificationChannelMapping = new HashMap<>();
        notificationChannelMapping.put(0, NotificationChannel.SMS);
        notificationChannelMapping.put(1, NotificationChannel.SMS);
        notificationChannelMapping.put(2, NotificationChannel.SMS);
        notificationChannelMapping.put(3, NotificationChannel.SMS);
        notificationChannelMapping.put(4, NotificationChannel.EMAIL);
        notificationChannelMapping.put(5, NotificationChannel.EMAIL);
        notificationChannelMapping.put(6, NotificationChannel.EMAIL);
        notificationChannelMapping.put(7, NotificationChannel.INBOX);
        notificationChannelMapping.put(8, NotificationChannel.INBOX);
        notificationChannelMapping.put(9, NotificationChannel.INBOX);
        return notificationChannelMapping;
    }
}
