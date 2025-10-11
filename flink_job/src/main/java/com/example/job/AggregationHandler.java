package com.example.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.sql.Timestamp;
import java.security.MessageDigest;
import java.util.Map;

public class AggregationHandler implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;
    
    // Data classes
    public static class Coordinate implements Serializable {
        private static final long serialVersionUID = 1L;
        public double lat;
        public double lon;
    }

    public static class Payload implements Serializable {
        private static final long serialVersionUID = 1L;
        public long ts;
        public Double speed;
        public Double speed_limit;
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

    public static class ValuableSensors implements Serializable {
        private static final long serialVersionUID = 1L;
        public boolean alarm;
        public Double vibration;
        public boolean rear_hatch_open;
        public boolean front_hatch_open;
        public boolean collision;
    }

    public static class FoodSensors implements Serializable {
        private static final long serialVersionUID = 1L;
        public boolean rear_hatch_open;  
        public boolean front_hatch_open;
        public Double temperature;  
        public Double humidity;
        public Double pressure;   
    }

    public static class Envelope implements Serializable {
        private static final long serialVersionUID = 1L;
        public long ts_unix;
        public String ts;
        public String dev_id;
        public String tag;
        public Payload payloadJson;
    }

    public static class AggregatedRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public long ts_unix;
        public Timestamp ts;
        public String dev_id;
        public String tag;
        public String route_hash;
        public String payloadJson;
    }
    
    // Aggregation state holder
    public static class AggregationState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int count = 0;
        public String route_hash = " ";
        public String tag = " ";
        public String status = " ";
        public String device_name = " ";
        public double delta_sum = 0.0;
        public double speed_limit = 0.0;
        public double sum_vibration = 0.0;
        public double sum_humidity = 0.0;
        public double sum_temperature = 0.0;
        public double sum_pressure = 0.0;
        public double avg_consumption = 0.0;
        public double min_consumption = 0.0;
        public double max_consumption = 0.0;
        public double avg_speed = 0.0;
        public double min_speed = 0.0;
        public double max_speed = 0.0;
        public boolean collision = false;
        public boolean is_food = false;
        public boolean is_valuable = false;
        public boolean alarm = false;
        public boolean is_pirate = false;
        public Envelope last_element = null;
    }
    
    public AggregationHandler() {
        initMapper();
    }
    
    private void initMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
    }
    
    public ObjectMapper getMapper() {
        initMapper();
        return mapper;
    }
    
    /**
     * Hashes route coordinates to create a unique identifier
     */
    public String hashCoordinates(Coordinate start, Coordinate end) {
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
    
    /**
     * Processes all elements for a single device and builds aggregation state
     */
    public AggregationState processElements(String key, Iterable<Envelope> elements) {
        initMapper(); // Ensure mapper is initialized
        AggregationState state = new AggregationState();
        
        for (Envelope env : elements) {
            state.last_element = env;
            
            if (env.payloadJson != null) {
                
                // Initialize on first element
                if (state.count == 0) {
                    state.speed_limit = env.payloadJson.speed_limit;
                    state.device_name = env.payloadJson.device_name;
                    state.tag = env.tag;
                    state.route_hash = hashCoordinates(env.payloadJson.start, env.payloadJson.end);
                }
                
                // Check for pirate behavior
                if (env.payloadJson.speed > env.payloadJson.speed_limit + 10) {
                    state.is_pirate = true;
                }

                state.count++;
                state.delta_sum += env.payloadJson.delta_distance;

                // Handle device-specific sensors
                if (env.payloadJson.device_type.equals("food") && env.payloadJson.sensors != null) {
                    state.is_food = true;
                    FoodSensors food_sensor = mapper.convertValue(env.payloadJson.sensors, FoodSensors.class);

                    if (food_sensor.humidity != null) state.sum_humidity += food_sensor.humidity;
                    if (food_sensor.pressure != null) state.sum_pressure += food_sensor.pressure;
                    if (food_sensor.temperature != null) state.sum_temperature += food_sensor.temperature;

                    System.out.printf(">>> [PARSED_FOOD OK] dev_type=%s%n", env.payloadJson.device_type);

                } else if (env.payloadJson.device_type.equals("valuable") && env.payloadJson.sensors != null) {
                    state.is_valuable = true;
                    ValuableSensors valuable_sensor = mapper.convertValue(env.payloadJson.sensors, ValuableSensors.class);

                    if (valuable_sensor.collision) state.collision = true;
                    if (valuable_sensor.alarm) state.alarm = true;
                    if (valuable_sensor.vibration != null) state.sum_vibration += valuable_sensor.vibration;

                    System.out.printf(">>> [PARSED_VALUABLE OK] dev_type=%s%n", env.payloadJson.device_type);
                }
            }
        }
        
        // Extract final stats from last element
        if (state.last_element != null) {
            state.status = state.last_element.payloadJson.status;
            Payload payload = state.last_element.payloadJson;

            state.avg_consumption = getDoubleValue(payload.consumption_stats, "avg");
            state.min_consumption = getDoubleValue(payload.consumption_stats, "min");
            state.max_consumption = getDoubleValue(payload.consumption_stats, "max");

            state.avg_speed = getDoubleValue(payload.speed_stats, "avg");
            state.min_speed = getDoubleValue(payload.speed_stats, "min");
            state.max_speed = getDoubleValue(payload.speed_stats, "max");
        }
        
        System.out.printf(">>> [WINDOW] dev_id=%s, count=%d%n", key, state.count);
        
        return state;
    }
    
    /**
     * Builds the final aggregated record from the state
     */
    public AggregatedRecord buildAggregatedRecord(String key, AggregationState state) {
        if (state.count == 0) {
            return null;
        }
        
        long now = System.currentTimeMillis();

        double avg_vibration = state.sum_vibration / state.count;
        double avg_pressure = state.sum_pressure / state.count;
        double avg_humidity = state.sum_humidity / state.count;
        double avg_temperature = state.sum_temperature / state.count;

        AggregatedRecord record = new AggregatedRecord();
        record.ts_unix = now / 1000L;
        record.ts = new Timestamp(now);
        record.route_hash = state.route_hash;
        record.dev_id = key;
        record.tag = state.tag;

        if (state.is_food) {
            record.payloadJson = buildFoodPayload(key, state, avg_humidity, avg_pressure, avg_temperature);
        } else if (state.is_valuable) {
            record.payloadJson = buildValuablePayload(key, state, avg_vibration);
        } else {
            record.payloadJson = buildGenericPayload(key, state);
        }

        return record;
    }
    
    private String buildFoodPayload(String key, AggregationState s, double avg_humidity, double avg_pressure, double avg_temperature) {
        System.out.printf(
                ">>> [AGGREGATED_FOOD] dev_id=%s avg_speed=%.2f count=%d delta_sum=%.2f speed_limit=%.2f " +
                "avg_humidity=%.2f avg_pressure=%.2f avg_temperature=%.2f min_speed=%.2f max_speed=%.2f " +
                "avg_consumption=%.2f min_consumption=%.2f max_consumption=%.2f status=%s device_name=%s is_pirate=%b%n",
                key, s.avg_speed, s.count, s.delta_sum, s.speed_limit,
                avg_humidity, avg_pressure, avg_temperature,
                s.min_speed, s.max_speed,
                s.avg_consumption, s.min_consumption, s.max_consumption,
                s.status, s.device_name, s.is_pirate);

        return String.format(
                "{\"avg_speed\": %.2f, \"count\": %d, \"delta_sum\": %.2f, \"speed_limit\": %.2f, " +
                "\"avg_humidity\": %.2f, \"avg_pressure\": %.2f, \"avg_temperature\": %.2f, " +
                "\"min_speed\": %.2f, \"max_speed\": %.2f, " +
                "\"avg_consumption\": %.2f, \"min_consumption\": %.2f, \"max_consumption\": %.2f, " +
                "\"status\": \"%s\", \"device_name\": \"%s\", \"is_pirate\": %b}",
                s.avg_speed, s.count, s.delta_sum, s.speed_limit,
                avg_humidity, avg_pressure, avg_temperature,
                s.min_speed, s.max_speed,
                s.avg_consumption, s.min_consumption, s.max_consumption,
                s.status, s.device_name, s.is_pirate);
    }

    private String buildValuablePayload(String key, AggregationState s, double avg_vibration) {
        System.out.printf(
                ">>> [AGGREGATED_VALUABLE] dev_id=%s avg_speed=%.2f count=%d delta_sum=%.2f speed_limit=%.2f " +
                "collision=%b alarm=%b avg_vibration=%.2f min_speed=%.2f max_speed=%.2f " +
                "avg_consumption=%.2f min_consumption=%.2f max_consumption=%.2f status=%s device_name=%s is_pirate=%b%n",
                key, s.avg_speed, s.count, s.delta_sum, s.speed_limit,
                s.collision, s.alarm, avg_vibration,
                s.min_speed, s.max_speed,
                s.avg_consumption, s.min_consumption, s.max_consumption,
                s.status, s.device_name, s.is_pirate);

        return String.format(
                "{\"avg_speed\": %.2f, \"count\": %d, \"delta_sum\": %.2f, \"speed_limit\": %.2f, " +
                "\"collision\": %b, \"alarm\": %b, \"avg_vibration\": %.2f, " +
                "\"min_speed\": %.2f, \"max_speed\": %.2f, " +
                "\"avg_consumption\": %.2f, \"min_consumption\": %.2f, \"max_consumption\": %.2f, " +
                "\"status\": \"%s\", \"device_name\": \"%s\", \"is_pirate\": %b}",
                s.avg_speed, s.count, s.delta_sum, s.speed_limit,
                s.collision, s.alarm, avg_vibration,
                s.min_speed, s.max_speed,
                s.avg_consumption, s.min_consumption, s.max_consumption,
                s.status, s.device_name, s.is_pirate);
    }

    private String buildGenericPayload(String key, AggregationState s) {
        System.out.printf(
                ">>> [AGGREGATED] dev_id=%s avg_speed=%.2f count=%d delta_sum=%.2f speed_limit=%.2f " +
                "max_speed=%.2f min_speed=%.2f avg_consumption=%.2f min_consumption=%.2f max_consumption=%.2f status=%s device_name=%s is_pirate=%b%n",
                key, s.avg_speed, s.count, s.delta_sum, s.speed_limit,
                s.max_speed, s.min_speed,
                s.avg_consumption, s.min_consumption, s.max_consumption,
                s.status, s.device_name, s.is_pirate);

        return String.format(
                "{\"avg_speed\": %.2f, \"count\": %d, \"delta_sum\": %.2f, \"speed_limit\": %.2f, " +
                "\"max_speed\": %.2f, \"min_speed\": %.2f, " +
                "\"avg_consumption\": %.2f, \"min_consumption\": %.2f, \"max_consumption\": %.2f, " +
                "\"status\": \"%s\", \"device_name\": \"%s\", \"is_pirate\": %b}",
                s.avg_speed, s.count, s.delta_sum, s.speed_limit,
                s.max_speed, s.min_speed,
                s.avg_consumption, s.min_consumption, s.max_consumption,
                s.status, s.device_name, s.is_pirate);
    }
    
    private double getDoubleValue(Map<String, Object> map, String key) {
        if (map == null || map.get(key) == null) {
            return 0.0;
        }
        return ((Number) map.get(key)).doubleValue();
    }
}
