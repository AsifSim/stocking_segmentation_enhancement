FROM openjdk:11-jre-slim

# Set the working directory
# WORKDIR /app
RUN echo"========================="
# Copy the built JAR file
COPY target/ToDocker/* /app

# Expose the desired port (modify as needed)
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["java", "-jar", "MyFlinkImage-1.0.jar"]
