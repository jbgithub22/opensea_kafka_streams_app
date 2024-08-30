import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Set;

public class KafkaTopicManager {

    private final AdminClient adminClient;

    public KafkaTopicManager() {
        this.adminClient = KafkaAdminClientFactory.createAdminClient();
    }

    public void createTopicIfNotExists(String topic) {
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().timeoutMs(10000));
            KafkaFuture<Set<String>> names = listTopicsResult.names();
            if (names.get().contains(topic)) {
                System.out.println("Topic " + topic + " already exists");
            } else {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic " + topic + " created");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        adminClient.close();
    }
}