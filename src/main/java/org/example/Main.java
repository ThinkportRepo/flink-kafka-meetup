package org.example;


import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import digital.thinkport.Stock;
import digital.thinkport.Trade;
import org.apache.flink.api.common.eventtime.*;
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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

public class Main {



    public static void main(String[] args) {

        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<Trade> source = KafkaSource.<Trade>builder()
                .setBootstrapServers("http://localhost:9092")
                .setTopics("trades")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(Trade.class, "http://localhost:8081"))
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("http://localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("trade-notification")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();



        SourceFunction<Stock> sourceFunction = PostgreSQLSource.<Stock>builder()
                .hostname("localhost")
                .port(5432)
                .database("stocks") // monitor postgres database
                .schemaList("prices")  // monitor inventory schema
                .tableList("prices.prices") // monitor products table
                .username("postgres")
                .password("postgres")
                .deserializer(new StockPriceDeserializer()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<Trade> stream = env.fromSource(source,WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStreamSource<Stock> streamDb = env.addSource(sourceFunction);


        stream.join(streamDb).where((KeySelector<Trade, String>) t -> t.getSymbol())
                .equalTo((KeySelector<Stock, String>) s -> s.getSymbol())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .apply((JoinFunction<Trade, Stock, String>) (trade, stock) -> {
                    String type = trade.getTypeOfTransaction().equals("BUY") ? "bought" : "sold";
                    Integer totalPrice = trade.getNumberOfStocks() * stock.getCurrentPrice();
                    String formatString = "%d stocks from %s %s for a total of %d";
                    String format = String.format(formatString, trade.getNumberOfStocks(), stock.getCompanyName(), type, totalPrice);
                    System.out.println(format);
                    return format;
                })
                .sinkTo(sink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}