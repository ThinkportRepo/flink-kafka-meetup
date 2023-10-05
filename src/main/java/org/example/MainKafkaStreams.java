package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import digital.thinkport.Stock;
import digital.thinkport.Trade;
import digital.thinkport.User;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class MainKafkaStreams {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-kafka-streams-app" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Serde<Stock> stockSerde = new SpecificAvroSerde<>(new CachedSchemaRegistryClient("http://localhost:8081", 100));
        Serde<Trade> tradeSerde = new SpecificAvroSerde<>(new CachedSchemaRegistryClient("http://localhost:8081", 100));

        HashMap<String, String> specificDeserializerProps = new HashMap<String, String>();
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        stockSerde.configure(specificDeserializerProps, false);
        tradeSerde.configure(specificDeserializerProps, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> streamDb = builder.stream("trades", Consumed.with(Serdes.String(),tradeSerde));
        KStream<String, Stock> streamKafka = builder.stream("connect.stocks", Consumed.with(Serdes.String(), stockSerde));

        ValueJoiner<Stock, Trade, String> userStringStringValueJoiner = (stock, trade) -> {
            String type = trade.getTypeOfTransaction().equals("BUY") ? "bought" : "sold";
            Integer totalPrice = trade.getNumberOfStocks() * stock.getCurrentPrice();
            String formatString = "%d stocks from %s %s for a total of %d";
            return String.format(formatString, trade.getNumberOfStocks(), stock.getCompanyName(), type, totalPrice);
        };
        streamKafka.join(streamDb, userStringStringValueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(1000)), StreamJoined.with(Serdes.String(), stockSerde, tradeSerde))
                .peek((key, value) -> System.out.println("joined value: " + value))
                .to("trade-notification-streams", Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}