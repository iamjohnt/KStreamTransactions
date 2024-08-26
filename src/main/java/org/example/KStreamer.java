package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.SECURITY_PROTOCOL_CONFIG;

public class KStreamer {

    private String inputTopic = "transactions";
    private String outputTopic = "filtered_transactions";
    private String confluentSecret;
    private String confluentBootstrapServer;
    final private String topic = "transactions";

    KStreamer(String confluentSecret, String confluentBootstrapServer) {
        this.confluentSecret = confluentSecret;
        this.confluentBootstrapServer = confluentBootstrapServer;
    }

    public void consume() {
        // create stream builder, and build Kstream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        // create Kstream topology
        stream.filter((key, value) -> Double.parseDouble(value) > 0)
                .peek((key, value) -> System.out.println("key " + key + " value " + value))
                .to(outputTopic);

        // create Kafka stream, and start streaming
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), createProperties());
        kafkaStreams.start();
    }

    private Properties createProperties() {
        return new Properties() {{
            put(BOOTSTRAP_SERVERS_CONFIG, confluentBootstrapServer);
            put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(ACKS_CONFIG, "all");
            put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            put(SASL_JAAS_CONFIG, confluentSecret);
            put(SASL_MECHANISM, "PLAIN");
            put(APPLICATION_ID_CONFIG, "kstream-filtering-1");
            put(CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        }};
    }
}
