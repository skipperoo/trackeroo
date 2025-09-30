# Stage 1: build
FROM gradle:8.7-jdk11 AS builder
WORKDIR /app
COPY . .
RUN gradle shadowJar --no-daemon

# Stage 2: runtime
FROM flink:1.20-scala_2.12  
WORKDIR /job
COPY --from=builder /app/build/libs/job-all.jar /job/job.jar

# Script per attendere il JobManager e sottomettere con parallelismo
RUN echo '#!/bin/bash\n\
echo "Waiting for Flink JobManager..."\n\
until curl -s http://flink-jobmanager:8081/overview; do\n\
  echo "JobManager not ready, retrying in 5s..."\n\
  sleep 5\n\
done\n\
\n\
echo "JobManager ready! Submitting job with parallelism ${FLINK_PARALLELISM:-16}"\n\
exec flink run \\\n\
  -m flink-jobmanager:8081 \\\n\
  --parallelism ${FLINK_PARALLELISM:-16} \\\n\
  -c com.example.job.AggregatorJob \\\n\
  /job/job.jar' > /job/start.sh \
    && chmod +x /job/start.sh

ENTRYPOINT ["/job/start.sh"]