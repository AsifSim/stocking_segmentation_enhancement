#FROM openjdk:11-jre-slim
#
## Set the working directory
#WORKDIR /app
#
## Copy the built JAR file
#COPY target/MyFlinkImage-1.0.jar /app/MyFlinkImage-1.0.jar
#
## Expose the desired port (modify as needed)
#EXPOSE 8080
#
## Command to run the application
#ENTRYPOINT ["java", "-jar", "MyFlinkImage-1.0.jar"]



FROM openjdk:11-jre-slim

# Set the working directory
 WORKDIR /app
#RUN echo"========================="
# Copy the built JAR file
COPY target/MyFlinkImage-1.0.jar /app/MyFlinkImage-1.0.jar
COPY src/main/resources/module.yaml /app/module.yaml

# Expose the desired port (modify as needed)
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["java", "-jar", "MyFlinkImage-1.0.jar"]
