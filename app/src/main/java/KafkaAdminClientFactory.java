import org.apache.kafka.clients.admin.AdminClient;
import java.util.Properties;

public class KafkaAdminClientFactory {

    private static final String KAFKA_ADDRESS = "localhost:9097";

    public static AdminClient createAdminClient() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", KAFKA_ADDRESS);
        return AdminClient.create(adminProps);
    }

    public static String getKafkaAddress() {
        return KAFKA_ADDRESS;
    }
}