package org.example;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import digital.thinkport.Stock;
import digital.thinkport.Trade;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class Flink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Trade> tradeKafkaSource = KafkaSource.<Trade>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("trades")
                .setGroupId("trades-group")
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(Trade.class, "http://localhost:8081"))
                .build();

        SourceFunction<Stock> stockSourceFunction = PostgreSQLSource.<Stock>builder()
                .hostname("localhost")
                .port(5432)
                .database("stocks")
                .schemaList("prices")
                .tableList("prices.prices")
                .username("postgres")
                .password("postgres")
                .deserializer(new StockPriceDeserializer())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("http://localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("trade-notification")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).build();


        DataStream<Trade> tradeDataStream = env.fromSource(tradeKafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
        DataStream<Stock> stockDataStream = env.addSource(stockSourceFunction);

        tradeDataStream.join(stockDataStream).where((KeySelector<Trade, String>) Trade::getSymbol)
                .equalTo((KeySelector<Stock, String>) Stock::getSymbol)
                .window(TumblingProcessingTimeWindows.of(Time.of(1000, TimeUnit.MILLISECONDS)))
                .apply((JoinFunction<Trade, Stock, String>) (trade, stock) -> {
                    String type = trade.getTypeOfTransaction().equals("BUY") ? "bought" : "sold";
                    Integer totalPrice = trade.getNumberOfStocks() * stock.getCurrentPrice();
                    String formatString = "%s %d %s stocks for a total of %d";
                    String result = String.format(formatString, type, trade.getNumberOfStocks(), stock.getCompanyName(), totalPrice);
                    return result;
                }).sinkTo(sink);


        env.execute();


    }
}
