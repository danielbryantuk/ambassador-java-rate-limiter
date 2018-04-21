FROM openjdk:8-jre
ADD target/simpleimpl-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 50051
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]