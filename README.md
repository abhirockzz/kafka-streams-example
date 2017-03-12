This is an example of a [Kafka Streams](https://kafka.apache.org/documentation/streams) based microservice (packaged in form of an Uber JAR). The scenario is simple

## Basics

- A producer application continuously emits CPU usage metrics into a Kafka topic (cpu-metrics-topic)	
- The consumer is a Kafka Streams application which uses the Processor (low level) Kafka Streams API to calculate the [Cumulative Moving Average](https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average) of the CPU metrics of each machine
- Consumers can be horizontally scaled - the processing work is distributed amongst many nodes and the process is elastic and flexible thanks to Kafka Streams (and the fact that it leverages Kafka for fault tolerance etc.)
- Each instance has its own (local) state for the calculated average. A custom REST API has been (using Jersey JAX-RS implementation) to tap into this state and provide a unified view of the entire system (moving averages of CPU usage of all machines)

### Setup

This project has two modules

- [Producer](https://github.com/abhirockzz/kafka-streams-example/tree/master/kafka-producer) application
- [Consumer](https://github.com/abhirockzz/kafka-streams-example/tree/master/kstreams-consumer) application (uses Kafka Streams)

## To try things out... 

- Start Kafka broker. Configure `num.partitions` in Kafka broker `server.properties` file to 5 (to experiment with this application)
- Build producer & consumer application - browse to the respective directory and execute `mvn clean install`
- Trigger producer application - `java -jar kafka-cpu-metrics-producer.jar`. It will start emitting records to Kafka
- Start one instance of consumer application - `java -jar kafka-cpu-metrics-consumer-1.0.jar -DKAFKA_CLUSTER=<kafka host:port>` (defaults to `localhost:9092` if not provided). Note the auto-selected port. It will start calculating the moving average of machine CPU metrics
- Access the metrics on this instance - `http://localhost:<inst1_port>/metrics`
- Start another instance of consumer application - `java -jar kafka-cpu-metrics-consumer-1.0.jar -DKAFKA_CLUSTER=<kafka host:port>` (note the auto-selected port), wait for a few seconds - the load will now be distributed amongst the two instances. Access the metrics `http://localhost:<inst2_port>/metrics` - you will see the metrics (JSON/XML payload) for all the machines as well as the instance on which the Cumulative Moving Average has been calculated

sample output - https://gist.github.com/abhirockzz/48e89873ae23c93d0a5cc721c87cc536

- You can also search for metrics for a specific machine ID `http://localhost:<any_app_port>/metrics/<machineID>`

sample output - https://gist.github.com/abhirockzz/2ca2297fbc9aec269d31707f61b4c45e

You can keep increasing the number of instances such that they are less than or equal to the number of partitions of your Kafka topic

> Having more instances than number of partitions is not going to have any effect on parallelism and that instance will be inactive until any of the existing instance is stopped   
