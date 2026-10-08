FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre
RUN groupadd -g 10001 app && useradd -u 10001 -g 10001 -r -s /usr/sbin/nologin app
WORKDIR /app
COPY --from=build --chown=10001:10001 /build/target/relay-0.1.0.jar app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
