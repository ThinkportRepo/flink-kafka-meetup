package org.example;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import digital.thinkport.Stock;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class StockPriceDeserializer implements DebeziumDeserializationSchema<Stock> {
    @Override
    public void deserialize(SourceRecord record, Collector<Stock> out) throws Exception {
        Struct value = ((Struct) record.value()).getStruct("after");
        String symbol = value.getString("symbol");
        String companyName = value.getString("companyname");
        int currentPrice = value.getInt32("currentprice");
        long timestamp = value.getInt64("timestamp");
        Stock stock = new Stock(currentPrice,symbol, companyName, timestamp);
        out.collect(stock);
    }

    @Override
    public TypeInformation<Stock> getProducedType() {
        return TypeInformation.of(Stock.class);
    }
}
