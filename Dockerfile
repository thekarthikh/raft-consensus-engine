FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn clean package -Dmaven.test.skip=true

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY --from=build /app/target/raft-engine-1.0-SNAPSHOT.jar app.jar

EXPOSE 50051

ENTRYPOINT ["java", "-jar", "app.jar"]
