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

public class AggregatorJob {

    public static class Envelope {
        public long ts_unix;
        public String ts;
        public String dev_id;
        public String tag;
        public Payload payload;
    }

    public static class Payload {
        public long ts;
        public double speed;
        public Object sensors;
    }

    public static class AggregatedRecord {
        public long ts_unix;
        public Timestamp ts;
        public String dev_id;
        public String tag;
        public String payloadJson;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(">>> [DEBUG] Starting Flink Job at " + Instant.now());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        Pattern topicPattern = Pattern.compile("j-data-.*");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setGroupId("flink-aggregator")
                .setTopicPattern(topicPattern)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperty("partition.discovery.interval.ms", "1000")
                .setProperty("auto.offset.reset", "latest")
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source with Regex"
        );

        rawStream.map(msg -> {
            System.out.printf(">>> [KAFKA_RAW] Received: %s%n", msg);
            return msg;
        }).map(msg -> {
            try {
                Envelope e = mapper.readValue(msg, Envelope.class);
                System.out.printf(">>> [PARSED] dev_id=%s tag=%s speed=%s%n",
                        e.dev_id,
                        e.tag,
                        e.payload != null ? e.payload.speed : "null");
                return e;
            } catch (Exception ex) {
                System.err.printf(">>> [PARSE_ERROR] %s - Message: %s%n", ex.getMessage(), msg);
                Envelope empty = new Envelope();
                empty.dev_id = "unknown";
                return empty;
            }
        }).filter(e -> {
            boolean valid = e.dev_id != null && !e.dev_id.equals("unknown");
            System.out.printf(">>> [FILTER] dev_id=%s valid=%b%n", e.dev_id, valid);
            return valid;
        }).setParallelism(1);  // parallelism 1 per debug facile

        // --- 1️⃣ DEBUG RAW KAFKA ---
        DataStream<String> debuggedStream = rawStream.map(value -> {
            System.out.printf(">>> [KAFKA_RAW] %s | Message length: %d%n", value, value.length());
            return value;
        }).setParallelism(1);

        // --- 2️⃣ PARSING ---
        DataStream<Envelope> parsed = debuggedStream.map(value -> {
            try {
                Envelope envObj = mapper.readValue(value, Envelope.class);
                System.out.printf(">>> [PARSING_SUCCESS] dev_id=%s, tag=%s, payload=%s%n",
                        envObj.dev_id,
                        envObj.tag,
                        envObj.payload != null ? "present" : "null");
                if (envObj.payload != null) {
                    System.out.printf(">>> [PAYLOAD] speed=%.2f, ts=%d%n",
                            envObj.payload.speed,
                            envObj.payload.ts);
                }
                return envObj;
            } catch (Exception e) {
                System.err.printf(">>> [PARSING_ERROR] %s | Message: %s%n", e.getMessage(), value);
                Envelope empty = new Envelope();
                empty.dev_id = "unknown";
                return empty;
            }
        }).setParallelism(1);

        // --- 3️⃣ FILTRO ---
        DataStream<Envelope> validParsed = parsed.filter(e -> {
            boolean isValid = e.dev_id != null && !e.dev_id.equals("unknown");
            System.out.printf(">>> [FILTER] dev_id=%s | valid=%b%n", e.dev_id, isValid);
            return isValid;
        }).setParallelism(1);

        // --- 4️⃣ AGGREGAZIONE ---
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
                            count++;
                            if (env.payload != null) {
                                sum += env.payload.speed;
                                if (env.payload.ts > maxTs) maxTs = env.payload.ts;
                                System.out.printf(">>> [WINDOW_ELEMENT] dev_id=%s, speed=%.2f, ts=%d%n",
                                        env.dev_id, env.payload.speed, env.payload.ts);
                            }
                        }

                        System.out.printf(">>> [WINDOW_SUMMARY] key=%s, count=%d, sum=%.2f%n", key, count, sum);

                        if (count > 0) {
                            double avg = sum / count;
                            AggregatedRecord record = new AggregatedRecord();
                            record.ts_unix = maxTs;
                            record.ts = Timestamp.from(Instant.ofEpochMilli(maxTs));
                            record.dev_id = key;
                            record.tag = "avg_speed";
                            record.payloadJson = String.format("{\"avg_speed\": %.2f, \"count\": %d}", avg, count);

                            System.out.printf(">>> [AGGREGATED] dev_id=%s, avg_speed=%.2f, count=%d%n",
                                    key, avg, count);

                            out.collect(record);
                        }
                    }
                }).setParallelism(1);

        // --- 5️⃣ SINK JDBC con debug ---
        aggregated.map(record -> {
            System.out.printf(">>> [SINK] Ready to insert: dev_id=%s, payload=%s%n",
                    record.dev_id, record.payloadJson);
            return record;
        }).addSink(JdbcSink.sink(
                "INSERT INTO trackeroo.aggregated (ts_unix, ts, dev_id, tag, payload) VALUES (?, ?, ?, ?, ?)",
                (ps, record) -> {
                    try {
                        ps.setLong(1, record.ts_unix);
                        ps.setTimestamp(2, record.ts);
                        ps.setString(3, record.dev_id);
                        ps.setString(4, record.tag);
                        ps.setString(5, record.payloadJson);
                        System.out.printf(">>> [DB_INSERT] dev_id=%s inserted successfully%n", record.dev_id);
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
        )).setParallelism(1);

        System.out.println(">>> [DEBUG] Job setup complete, executing...");
        env.execute("Dynamic Kafka Aggregator Job with Debug");
    }
}


/* 
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

public class AggregatorJob {

    public static class Envelope {
        public long ts_unix;
        public String ts;
        public String dev_id;
        public String tag;
        public Payload payload;
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

    public static class Coordinate {
        public double lat;
        public double lon;
    }

    public static class AggregatedRecord {
        public long ts_unix;
        public Timestamp ts;
        public String dev_id;
        public String tag;
        public String payloadJson;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(">>> Starting Flink Job with Dynamic Topic Discovery!");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        // Pattern regex per il dynamic topic discovery 
        Pattern topicPattern = Pattern.compile("j-data-.*");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setGroupId("flink-aggregator")
                .setTopicPattern(topicPattern)  // Usa regex per i topick
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperty("partition.discovery.interval.ms", "5000")  // Dynamic discovery ogni 5 s
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source with Regex"
        );

        DataStream<String> debuggedStream = rawStream.map(value -> {
            System.out.printf(">>> Raw Kafka Message: %s%n", value);
            return value;
        });

        DataStream<Envelope> parsed = debuggedStream.map(value -> {
            try {
                return mapper.readValue(value, Envelope.class);
            } catch (Exception e) {
                System.err.printf(">>> JSON Parse Error: %s - Message: %s%n", e.getMessage(), value);
                Envelope empty = new Envelope();
                empty.dev_id = "unknown";
                return empty;
            }
        });

        DataStream<Envelope> validParsed = parsed.filter(envelope ->
                envelope.dev_id != null && !envelope.dev_id.equals("unknown")
        );

        DataStream<AggregatedRecord> aggregated = validParsed
                .keyBy(envelope -> envelope.dev_id)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))  // Finestra con Duration :cite[2]
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

                        if (count > 0) {
                            double avg = sum / count;
                            AggregatedRecord record = new AggregatedRecord();
                            record.ts_unix = maxTs;
                            record.ts = Timestamp.from(Instant.ofEpochMilli(maxTs));
                            record.dev_id = key;
                            record.tag = "avg_speed";
                            record.payloadJson = String.format("{\"avg_speed\": %.2f}", avg);
                            out.collect(record);
                        }
                    }
                });

        aggregated.addSink(JdbcSink.sink(
                "INSERT INTO trackeroo.aggregated (ts_unix, ts, dev_id, tag, payload) VALUES (?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setLong(1, record.ts_unix);
                    ps.setTimestamp(2, record.ts);
                    ps.setString(3, record.dev_id);
                    ps.setString(4, record.tag);
                    ps.setString(5, record.payloadJson);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://tsdb:5432/tracker_db")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("postgres")
                        .withPassword("postgres")
                        .build()
        ));

        env.execute("Dynamic Kafka Aggregator Job");
    }
} 

*/