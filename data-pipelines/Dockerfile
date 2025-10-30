FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/scala-2.13/data-pipelines-assembly-0.1.0.jar app.jar

CMD ["java","-Xmx2g","-jar","app.jar"]