import org.apache.kafka.clients.admin.AdminClient;
import java.util.Properties;

public class KafkaAdminClientFactory {

    public static AdminClient createAdminClient(String bootstrapServers) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        return AdminClient.create(adminProps);
    }
}