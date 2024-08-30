import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

public class KafkaStreamsApp {

    public static void main(String[] args) {
        // Set up the configuration properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9097");

        // Create a StreamsBuilder object
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream object by subscribing to the input topic
        KStream<String, String> inputTopic = builder.stream("test-os_event");

        // Create an ObjectMapper for parsing JSON
        ObjectMapper objectMapper = new ObjectMapper();

        // Process the input stream and split into different topics
        inputTopic.mapValues(value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                JsonNode payload = jsonNode.get("payload");

                // Extract data for time_series_data topic
                JsonNode timeSeriesData = objectMapper.createObjectNode()
                        .put("event_timestamp", payload.get("event_timestamp").asText())
                        .put("closing_date", payload.get("closing_date").asText())
                        .put("transaction_hash", payload.get("transaction").get("hash").asText());

                // Extract data for minimal_schema topic
                JsonNode minimalSchema = objectMapper.createObjectNode()
                        .put("event_type", jsonNode.get("event_type").asText())
                        .put("nft_id", payload.get("item").get("nft_id").asText())
                        .put("sale_price", payload.get("sale_price").asText());

                // Extract data for text_data topic
                JsonNode textData = objectMapper.createObjectNode()
                        .put("collection_slug", payload.get("collection").get("slug").asText())
                        .put("maker_address", payload.get("maker").get("address").asText())
                        .put("taker_address", payload.get("taker").get("address").asText());

                // Extract data for nft_images topic
                JsonNode nftImages = objectMapper.createObjectNode()
                        .put("permalink", payload.get("item").get("permalink").asText());

                // Send the extracted data to respective topics
                builder.stream("time_series_data").to("time_series_data");
                builder.stream("minimal_schema").to("minimal_schema");
                builder.stream("text_data").to("text_data");
                builder.stream("nft_images").to("nft_images");

                return value;
            } catch (Exception e) {
                e.printStackTrace();
                return value;
            }
        });

        // Build the KafkaStreams object with the configuration properties and the StreamsBuilder
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Start the KafkaStreams application
        streams.start();

        // Add shutdown hook to gracefully close the KafkaStreams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}