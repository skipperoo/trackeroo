# Stage 1: build
FROM gradle:8.7-jdk11 AS builder
WORKDIR /app
COPY . .
RUN gradle shadowJar --no-daemon

# Stage 2: runtime
FROM flink:1.20.0-scala_2.12  
WORKDIR /job
COPY --from=builder /app/build/libs/job-all.jar /job/job.jar

# Script per attendere il JobManager e sottomettere con parallelismo
COPY <<'EOF' /job/start.sh
#!/bin/bash
set -e
echo "Waiting for Flink JobManager at ${FLINK_JM_HOST}:${FLINK_JM_PORT}..."
until curl -s http://${FLINK_JM_HOST}:${FLINK_JM_PORT}/overview; do
  echo "JobManager not ready, retrying in 5s..."
  sleep 5
done
echo "JobManager ready! Submitting job with parallelism ${FLINK_PARALLELISM}"
exec /opt/flink/bin/flink run \
  -m ${FLINK_JM_HOST}:${FLINK_JM_PORT} \
  --parallelism ${FLINK_PARALLELISM} \
  -c com.example.job.AggregatorJob \
  /job/job.jar
EOF

RUN chmod +x /job/start.sh
ENTRYPOINT ["/job/start.sh"]