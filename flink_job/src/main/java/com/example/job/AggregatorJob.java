package com.example.job;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.job.AggregationHandler.AggregatedRecord;
import com.example.job.AggregationHandler.AggregationState;
import com.example.job.AggregationHandler.Envelope;

public class AggregatorJob {
    
    private static class AggregationProcessFunction 
            extends ProcessWindowFunction<Envelope, AggregatedRecord, String, TimeWindow> 
            implements Serializable {
        
        private static final long serialVersionUID = 1L;
        private final AggregationHandler handler;
        
        public AggregationProcessFunction(AggregationHandler handler) {
            this.handler = handler;
        }
        
        @Override
        public void process(String key, Context context, Iterable<Envelope> elements, 
                          Collector<AggregatedRecord> out) {
            
            AggregationState state = handler.processElements(key, elements);
            
            AggregatedRecord record = handler.buildAggregatedRecord(key, state);
            
            if (record != null) {
                out.collect(record);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(">>> [START] Flink Job at " + Instant.now());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final AggregationHandler handler = new AggregationHandler();

        Pattern topicPattern = Pattern.compile("j-data-.*");

        // Kafka Source configuration
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setGroupId("flink-aggregator")
                .setTopicPattern(topicPattern)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperty("partition.discovery.interval.ms", "5000")
                .setProperty("fetch.max.wait.ms", "500")
                .build();

        // Raw stream from Kafka
        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source with Regex"
        ).setParallelism(2);

        // Parse JSON messages
        DataStream<Envelope> parsed = rawStream.map(value -> {
            System.out.printf(">>> [KAFKA_MSG] Received at %s: %s%n", Instant.now(), value);
            try {
                Envelope envObj = handler.getMapper().readValue(value, Envelope.class);
                System.out.printf(">>> [PARSED_OK] dev_id=%s tag=%s%n", envObj.dev_id, envObj.tag);
                return envObj;
            } catch (Exception e) {
                System.err.printf(">>> [JSON_PARSE_ERROR] %s - Message: %s%n", e.getMessage(), value);
                Envelope empty = new Envelope();
                empty.dev_id = "unknown";
                return empty;
            }
        }).setParallelism(4);

        // Filter valid messages
        DataStream<Envelope> validParsed = parsed.filter(e ->
                e.dev_id != null && !e.dev_id.equals("unknown")
        ).setParallelism(2);

        // Aggregate by device_id in 10-second windows
        DataStream<AggregatedRecord> aggregated = validParsed
                .keyBy(e -> e.dev_id)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .process(new AggregationProcessFunction(handler))
                .setParallelism(6);

        // Sink to PostgreSQL
        aggregated.addSink(JdbcSink.sink(
                "INSERT INTO trackeroo.aggregated (ts_unix, ts, dev_id, route_hash, insertion_time, tag, payload) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (route_hash, ts ,dev_id) DO UPDATE SET " +
                "ts_unix = EXCLUDED.ts_unix, " +
                "ts = EXCLUDED.ts, " +
                "insertion_time = NOW(), " +
                "payload = EXCLUDED.payload",

                (ps, record) -> {
                    try {
                        ps.setLong(1, record.ts_unix);                       
                        ps.setTimestamp(2, record.ts);                       
                        ps.setString(3, record.dev_id);                      
                        ps.setString(4, record.route_hash);                  
                        ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                        ps.setString(6, record.tag);                        
                        ps.setObject(7, record.payloadJson, java.sql.Types.OTHER); 
                        System.out.printf(">>> [DB_INSERT/UPSERT] dev_id=%s route=%s ok%n", 
                                record.dev_id, record.route_hash);
                    } catch (Exception e) {
                        System.err.printf(">>> [DB_ERROR] dev_id=%s route=%s | %s%n",
                                record.dev_id, record.route_hash, e.getMessage());
                        throw e;
                    }
                },    

                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://tsdb:5432/tracker_db?sslmode=disable")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("admin")
                        .withPassword("administrator")
                        .build()
        )).setParallelism(2);

        env.execute("Dynamic Kafka Aggregator Job (with minimal debug)");
    }
}