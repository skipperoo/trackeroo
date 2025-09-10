package com.example.job;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import java.security.MessageDigest;

// aggiungere consumo massimo, consumo avg, speed massima, avg speed, speed limit 
public class AggregatorJob {

    public static class Coordinate {
        public double lat;
        public double lon;
    }

    public static String hashCoordinates(Coordinate start, Coordinate end) {
        try {
            String input = start.lat + "," + start.lon + ";" + end.lat + "," + end.lon;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes());

            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                hexString.append(String.format("%02x", b));
            }

            return hexString.toString().substring(0, 16);
        } catch (Exception e) {
            throw new RuntimeException("Error computing route hash", e);
        }
    }


    public static class Payload {
        public long ts;
        public double speed;
        public double speed_limit;
        public Map<String, Object> speed_stats;
        public Coordinate position;
        public String device_name;
        public String device_type;
        public String device_id;
        public String status;
        public Object sensors;
        public Coordinate start;
        public Coordinate end;
        public double instant_consumption;
        public Map<String, Object> consumption_stats;
        public double delta_distance;
    }

    public static class Envelope {
        public long ts_unix;
        public String ts;
        public String dev_id;
        public String tag;
        public Payload payloadJson;
    }

    public static class AggregatedRecord {
        public long ts_unix;
        public Timestamp ts;
        public String dev_id;
        public String tag;
        public String route_hash;
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
                        String route_hash = " ";
                        double sum = 0.0;
                        long maxTs = 0;

                        for (Envelope env : elements) {
                            if (count == 0) {
                                route_hash = hashCoordinates(env.payloadJson.start, env.payloadJson.end);
                            }
                            if (env.payloadJson != null) {
                                count++;
                                sum += env.payloadJson.speed;
                                if (env.payloadJson.ts > maxTs) maxTs = env.payloadJson.ts;
                            }
                        }

                        System.out.printf(">>> [WINDOW] dev_id=%s, count=%d%n", key, count);

                        if (count > 0) {
                            double avg = sum / count;
                            AggregatedRecord record = new AggregatedRecord();
                            record.ts_unix = maxTs;
                            record.ts = Timestamp.from(Instant.ofEpochMilli(maxTs));
                            record.route_hash = route_hash;
                            record.dev_id = key;
                            record.tag = "avg_speed";
                            record.payloadJson = String.format("{\"avg_speed\": %.2f, \"count\": %d}", avg, count);

                            System.out.printf(">>> [AGGREGATED] dev_id=%s avg_speed=%.2f count=%d%n", key, avg, count);
                            out.collect(record);
                        }
                    }
                });

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
                        ps.setLong(1, record.ts_unix);                       // ts_unix
                        ps.setTimestamp(2, record.ts);                       // ts
                        ps.setString(3, record.dev_id);                      // dev_id
                        ps.setString(4, record.route_hash);                  // route_hash (NEW)
                        ps.setTimestamp(5, new Timestamp(System.currentTimeMillis())); // insertion_time
                        ps.setString(6, record.tag);                         // tag
                        ps.setObject(7, record.payloadJson, java.sql.Types.OTHER); // payload jsonb
                        System.out.printf(">>> [DB_INSERT/UPSERT] dev_id=%s route=%s ok%n", record.dev_id, record.route_hash);
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
                ));

                env.execute("Dynamic Kafka Aggregator Job (with minimal debug)");
    }
}
