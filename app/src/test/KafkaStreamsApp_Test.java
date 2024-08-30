import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class KafkaStreamsApp_Test {

    public static void main(String[] args) {
        // Set up the configuration properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9097");

        // Create a StreamsBuilder object
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream object by subscribing to the input topic
        KStream<String, String> inputTopic = builder.stream("input-topic");

        // Split the messages into four other topics based on some condition
        inputTopic
                .filter((key, value) -> value.contains("condition1"))
                .to("output-topic1");

        inputTopic
                .filter((key, value) -> value.contains("condition2"))
                .to("output-topic2");

        inputTopic
                .filter((key, value) -> value.contains("condition3"))
                .to("output-topic3");

        inputTopic
                .filter((key, value) -> value.contains("condition4"))
                .to("output-topic4");

        // Build the KafkaStreams object with the configuration properties and the StreamsBuilder
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Start the KafkaStreams application
        streams.start();

        // Add shutdown hook to gracefully close the KafkaStreams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}