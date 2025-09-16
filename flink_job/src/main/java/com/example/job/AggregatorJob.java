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

    public static class ValuableSensors {
    	public boolean alarm;
    	public double vibration;
    	public boolean rear_hatch_open;
    	public boolean front_hatch_open;
    	public boolean collision;
    }

    public static class FoodSensors {
    	public boolean rear_hatch_open;  
    	public boolean front_hatch_open;
    	public Double temperature;  
    	public Double humidity;
    	public Double pressure;   
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
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(4)))
                .process(new ProcessWindowFunction<Envelope, AggregatedRecord, String, TimeWindow>() {

                    @Override
                    public void process(String key, Context context, Iterable<Envelope> elements, Collector<AggregatedRecord> out) {
                        Envelope last_element = null;
                        int count = 0;
                        String route_hash = " ";
                        String tag = " ";
                        String status = " ";
                        double delta_sum = 0.0;
                        double speed_limit = 0.0;
                        double sum_vibration = 0.0;
                        double sum_humidity = 0.0;
                        double sum_temperature = 0.0;
                        double sum_pressure = 0.0;
                        double avg_consumption = 0.0;
                        double min_consumption = 0.0;
                        double max_consumption = 0.0;
                        double avg_speed = 0.0;
                        double min_speed = 0.0;
                        double max_speed = 0.0;
                        boolean collision = false;
                        boolean is_food = false;
                        boolean is_valuable = false;
                        boolean alarm = false;
 
                        /* si cicla gli elementi della window */
                        for (Envelope env : elements) {
                            last_element = env;
                            if (env.payloadJson != null) {
                                if (count == 0) {
                                    speed_limit = env.payloadJson.speed_limit;
                                    tag = env.tag;
                                    route_hash = hashCoordinates(env.payloadJson.start, env.payloadJson.end);
                                }
                                count++;
                                delta_sum += env.payloadJson.delta_distance;
                                if (env.payloadJson.device_type.equals("food") && env.payloadJson.sensors != null){
                                    is_food = true;
                                    //FoodSensors food_sensor = mapper.readValue(env.sensors, FoodSensors.class);
                                    FoodSensors food_sensor = mapper.convertValue(env.payloadJson.sensors, FoodSensors.class);
                                    sum_humidity += food_sensor.humidity;
                                    sum_pressure += food_sensor.pressure;
                                    sum_temperature += food_sensor.temperature;

                                    System.out.printf(">>> [PARSED_FOOD OK] dev_type=%s%n", env.payloadJson.device_type);
                                } else if (env.payloadJson.device_type.equals("valuable") && env.payloadJson.sensors != null) {
                                    is_valuable = true;
                                    //ValuableSensors valuable_sensor = mapper.readValue(env.sensors, ValuableSensors.class);
                                    ValuableSensors valuable_sensor = mapper.convertValue(env.payloadJson.sensors, ValuableSensors.class);
                                    if (valuable_sensor.collision)
                                        collision = true;
                                    if (valuable_sensor.alarm)
                                        alarm = true;
                                    sum_vibration += valuable_sensor.vibration;

                                    System.out.printf(">>> [PARSED_VALUABLE OK] dev_type=%s%n", env.payloadJson.device_type);
                                }
                            }
                        }

                        if (last_element != null) {
                            status = last_element.payloadJson.status;
                            Payload payload = last_element.payloadJson;
                            avg_consumption = payload.consumption_stats.get("avg") != null ? ((Number) payload.consumption_stats.get("avg")).doubleValue() : 0.0;
                            min_consumption = payload.consumption_stats.get("min") != null ? ((Number) payload.consumption_stats.get("min")).doubleValue() : 0.0;
                            max_consumption = payload.consumption_stats.get("max") != null ? ((Number) payload.consumption_stats.get("max")).doubleValue() : 0.0;

                            avg_speed = payload.speed_stats.get("avg") != null ? ((Number) payload.speed_stats.get("avg")).doubleValue() : 0.0;
                            min_speed = payload.speed_stats.get("min") != null ? ((Number) payload.speed_stats.get("min")).doubleValue() : 0.0;
                            max_speed = payload.speed_stats.get("max") != null ? ((Number) payload.speed_stats.get("max")).doubleValue() : 0.0;


                        }

                        System.out.printf(">>> [WINDOW] dev_id=%s, count=%d%n", key, count);

                        if (count > 0) {
                            long now = System.currentTimeMillis();
                            double avg_vibration = sum_vibration / count;
                            double avg_pressure = sum_pressure / count;
                            double avg_humidity = sum_humidity / count;
                            double avg_temperature = sum_temperature / count;
                            AggregatedRecord record = new AggregatedRecord();
                            record.ts_unix = now / 1000L; /* in seconds */
                            record.ts = new Timestamp(now);
                            record.route_hash = route_hash;
                            record.dev_id = key;
                            record.tag = tag;

                            if (is_food) {
                                record.payloadJson = String.format("{\"avg_speed\": %.2f, \"count\": %d, \"delta_sum\": %.2f, \"speed_limit\": %.2f, \"avg_humidity\": %.2f, \"avg_pressure\": %.2f, \"avg_temperature\": %.2f}", avg_speed, count, delta_sum, speed_limit, avg_humidity, avg_pressure, avg_temperature);
                                System.out.printf(">>> [AGGREGATED_FOOD] dev_id=%s avg_speed=%.2f count=%d delta_sum=%.2f speed_limit=%.2f%n", key, avg_speed, count, delta_sum, speed_limit);
                            } else if (is_valuable) {
                                record.payloadJson = String.format("{\"avg_speed\": %.2f, \"count\": %d, \"delta_sum\": %.2f, \"speed_limit\": %.2f, \"collision\": %b, \"alarm\": %b, \"avg_vibration\": %.2f}", avg_speed, count, delta_sum, speed_limit, collision, alarm, avg_vibration);
                                System.out.printf(">>> [AGGREGATED_VALUABLE] dev_id=%s avg_speed=%.2f count=%d delta_sum=%.2f speed_limit=%.2f%n", key, avg_speed, count, delta_sum, speed_limit);
                            } else {
                                record.payloadJson = String.format("{\"avg_speed\": %.2f, \"count\": %d, \"delta_sum\": %.2f, \"speed_limit\": %.2f, \"max_speed\": %.2f, \"min_speed\": %.2f, \"avg_consumption\": %.2f, \"min_consumption\": %.2f, \"max_consumption\": %.2f}", avg_speed, count, delta_sum, speed_limit, max_speed, min_speed, avg_consumption, min_consumption, max_consumption);

                                System.out.printf(">>> [AGGREGATED] dev_id=%s avg_speed_speed=%.2f count=%d delta_sum=%.2f speed_limit=%.2f%n", key, avg_speed, count, delta_sum, speed_limit);
                            }


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


                    // fato TODO: mettere il nostro timestamp
                    // TODO: a seconda del device type leggere il sensore giusto e aggregare.  Solo valueable e food hanno il sensore, 
                    // mettere la somma dei delta per la distanza totale.
                    // TODO: grafana, dash board mappa generale con magari drop down dove si seglie il device
                    // TODO: dashboard di statistiche generale di tutto il sistema, quanti pirati. Cibi marci, o scassinamento. 
                    // TODO: dashboard di soli eventi, quindi una temperatura sopra un certa soglia o il sensore sopra. 
                    // TODO: piu tabelle per device anche aggregati e anche quelle normali su brookeroo quando esiste. (controllare e fare solamente una volta if not exist table).

                    /*
                    senti mi aggreghi la stringa status anche dell'ultimo del ciclo elements, sempre dell'ultimo se aggiungi ai dati aggregati la sua instant consuption, e le consumption stat che su go sono date da questa funzione se riesci a capirci: func (sv *StatVar[T]) Get() map[string]any { res := map[string]any{ "avg_speed": sv.Avg_speed, "min": sv.Min, "max": sv.Max, "count": sv.Count, } sv.Avg_speed = 0.0 sv.Min = 0 sv.Max = 0 sv.Count = 0 return res } poi se puoi aggregare nei valuable sensors, se c'è stata un'allarme allora lo aggrega, la vibrazione la piu grande vista, se c'è stata una collisione mentre nei food sensors prendi la temperatura media, la umidita media e la pressione media.
                    */