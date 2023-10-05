package org.example;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import digital.thinkport.Stock;
import digital.thinkport.Trade;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Trade> tradeSource = KafkaSource.<Trade>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("flink-trade-group")
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(Trade.class, "http://localhost:8081"))
                .setTopics("trades")
                .build();

        SourceFunction<Stock> stockSourceFunction = PostgreSQLSource.<Stock>builder()
                .hostname("localhost")
                .port(5432)
                .username("postgres")
                .password("postgres")
                .schemaList("prices")
                .tableList("prices.prices")
                .database("stocks")
                .deserializer(new StockPriceDeserializer())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                                .setTopic("notify-stocks")
                                .build()
                                ).build();
        DataStream<Trade> trades = env.fromSource(tradeSource, WatermarkStrategy.noWatermarks(), "TradeSource");
        DataStream<Stock> stocks = env.addSource(stockSourceFunction, "Stocks Source");

        trades.join(stocks).where(Trade::getSymbol)
                .equalTo(Stock::getSymbol)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .apply((trade, stock) -> {
                    // 5 Apple stocks bought for 10000 dollar
                    int totalPrice = trade.getNumberOfStocks() * stock.getCurrentPrice();
                    String type = trade.getTypeOfTransaction().equals("BUY") ? "bought" : "sold";
                    String formatString = "%d %s %s for %d dollar";
                    String result = String.format(formatString, trade.getNumberOfStocks(), stock.getCompanyName(), type, totalPrice);
                    System.out.println(result);
                    return result;
                }).sinkTo(sink);

        env.execute();
    }
}
