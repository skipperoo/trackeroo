# Stage 1: build
FROM gradle:8.7-jdk11 AS builder
WORKDIR /app
COPY . .
RUN gradle shadowJar --no-daemon

# Stage 2: runtime
FROM flink:1.20-scala_2.12  
WORKDIR /job
COPY --from=builder /app/build/libs/job-all.jar /job/job.jar

# Script per attendere il JobManager
RUN echo '#!/bin/bash\n\
until curl -s http://flink-jobmanager:8081/overview; do\n\
  echo "Waiting for Flink JobManager..."\n\
  sleep 5\n\
done\n\
exec flink run -m flink-jobmanager:8081 -c com.example.job.AggregatorJob /job/job.jar' > /job/start.sh \
    && chmod +x /job/start.sh

ENTRYPOINT ["/job/start.sh"]