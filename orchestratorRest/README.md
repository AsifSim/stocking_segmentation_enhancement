To start the code
1: Build the docker(docker-compose build)

2: docker-compose up

To initiate the flow, Hit the endpoint

http://127.0.0.1:8090/org.example/greetings/1?content-type=application/vnd.greeter.types/Greet

with JSON payload { "who":"AzmiAsif", "Greeting":"Good Night" }

OR

curl -X PUT http://127.0.0.1:8090/org.example/greetings/8
-H "Content-Type: application/vnd.greeter.types/Greet"
-d '{"who":"AzmiAsif","Greeting":"Good Night"}'
