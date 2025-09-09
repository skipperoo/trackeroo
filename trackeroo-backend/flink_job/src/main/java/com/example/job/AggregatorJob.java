package com.example.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
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

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Pattern;

// aggiungere consumo massimo, consumo avg, speed massima, avg speed, speed limit 
public class AggregatorJob {

    public static class Coordinate {
        public double lat;
        public double lon;
    }

    public static class Payload {
        public long ts;
        public double speed;
        public Coordinate position;
        public String device_type;
        public String status;
        public Object sensors;
        public Coordinate start;
        public Coordinate end;
    }

    public static class Envelope {
        public long ts_unix;
        public String ts;
        public String dev_id;
        public String tag;
        public Payload payload;
    }

    public static class AggregatedRecord {
        public long ts_unix;
        public Timestamp ts;
        public String dev_id;
        public String tag;
        public String payloadJson;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(">>> [START] Flink Job at " + Instant.now());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        Pattern topicPattern = Pattern.compile("j-data-.*");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setGroupId("flink-aggregator")
                .setTopicPattern(topicPattern)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperty("partition.discovery.interval.ms", "5000")
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source with Regex"
        );

        // --- Parsing con logging ---
        DataStream<Envelope> parsed = rawStream.map(value -> {
            System.out.printf(">>> [KAFKA_MSG] Received at %s: %s%n", Instant.now(), value);
            try {
                Envelope envObj = mapper.readValue(value, Envelope.class);
                System.out.printf(">>> [PARSED_OK] dev_id=%s tag=%s%n", envObj.dev_id, envObj.tag);
                return envObj;
            } catch (Exception e) {
                System.err.printf(">>> [JSON_PARSE_ERROR] %s - Message: %s%n", e.getMessage(), value);
                Envelope empty = new Envelope();
                empty.dev_id = "unknown";
                return empty;
            }
        });

        // --- Filtro per record validi ---
        DataStream<Envelope> validParsed = parsed.filter(e ->
                e.dev_id != null && !e.dev_id.equals("unknown")
        );

        // --- Aggregazione ---
        DataStream<AggregatedRecord> aggregated = validParsed
                .keyBy(e -> e.dev_id)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .process(new ProcessWindowFunction<Envelope, AggregatedRecord, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Envelope> elements, Collector<AggregatedRecord> out) {
                        int count = 0;
                        double sum = 0.0;
                        long maxTs = 0;

                        for (Envelope env : elements) {
                            if (env.payload != null) {
                                count++;
                                sum += env.payload.speed;
                                if (env.payload.ts > maxTs) maxTs = env.payload.ts;
                            }
                        }

                        System.out.printf(">>> [WINDOW] dev_id=%s, count=%d%n", key, count);

                        if (count > 0) {
                            double avg = sum / count;
                            AggregatedRecord record = new AggregatedRecord();
                            record.ts_unix = maxTs;
                            record.ts = Timestamp.from(Instant.ofEpochMilli(maxTs));
                            record.dev_id = key;
                            record.tag = "avg_speed";
                            record.payloadJson = String.format("{\"avg_speed\": %.2f, \"count\": %d}", avg, count);

                            System.out.printf(">>> [AGGREGATED] dev_id=%s avg_speed=%.2f count=%d%n", key, avg, count);
                            out.collect(record);
                        }
                    }
                });

            aggregated.addSink(JdbcSink.sink(
                "INSERT INTO trackeroo.aggregated (ts_unix, ts, dev_id, insertion_time, tag, payload) " +
                "VALUES (?, ?, ?, ?, ?, ?)",

                (ps, record) -> {
                    try {
                        ps.setLong(1, record.ts_unix);
                        ps.setTimestamp(2, record.ts);                // ts (PK)
                        ps.setString(3, record.dev_id);               // dev_id (PK)
                        ps.setTimestamp(4, new Timestamp(System.currentTimeMillis())); // insertion_time = now()
                        ps.setString(5, record.tag);                  // tag
                        ps.setObject(6, record.payloadJson, java.sql.Types.OTHER); // payload jsonb
                        System.out.printf(">>> [DB_INSERT] dev_id=%s ok%n", record.dev_id);
                    } catch (Exception e) {
                        System.err.printf(">>> [DB_ERROR] dev_id=%s | %s%n", record.dev_id, e.getMessage());
                        throw e;
                    }
                },

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:postgresql://tsdb:5432/tracker_db")
                    .withDriverName("org.postgresql.Driver")
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .build()
                ));

             env.execute("Dynamic Kafka Aggregator Job (with minimal debug)");
    }
}
