# Two-client OddSockets Java SDK demo, built straight from the SDK sources.
#
# Build (from the repo root):
#   docker build -f demo/Dockerfile -t oddsockets-java-demo .
#
# Basic pub/sub round-trip (default entrypoint):
#   docker run --rm -e ODDSOCKETS_API_KEY=ak_your_key_here oddsockets-java-demo
#
# Enhanced-events two-client receive-path regression:
#   docker run --rm --entrypoint /usr/local/bin/enhanced \
#     -e ODDSOCKETS_API_KEY=ak_your_key_here oddsockets-java-demo
FROM maven:3.9-eclipse-temurin-17

WORKDIR /app
COPY . .

# Build the SDK fat jar, then compile the two demo entrypoints against it.
RUN mvn -q -DskipTests package \
 && cp target/oddsockets-java-sdk-*-jar-with-dependencies.jar /app/sdk.jar \
 && javac -cp /app/sdk.jar -d /app/demo/out demo/BasicDemo.java demo/EnhancedDemo.java

# Launcher scripts so the two entrypoints can be selected like the go demo.
RUN printf '#!/bin/sh\nexec java -cp /app/sdk.jar:/app/demo/out BasicDemo\n' > /usr/local/bin/demo \
 && printf '#!/bin/sh\nexec java -cp /app/sdk.jar:/app/demo/out EnhancedDemo\n' > /usr/local/bin/enhanced \
 && chmod +x /usr/local/bin/demo /usr/local/bin/enhanced

ENTRYPOINT ["/usr/local/bin/demo"]
