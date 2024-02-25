* Sometimes the acknowledgement of kafka message being processed by @KafkaListener may not be delivered to the broker (i.e. temporary connection problem). In the end after the timeout specified it gets re-sent by the producer. This way at-least-once delivery may happen. It can be simulated. For example I could create an app that based on its context loaded is sending a certain, prepared set of data that can be send multiple times if the app is being re started over and over again.
* There is a solution - in-memory cache (as external service i.e. running on Docker)
* We can save the processed messages ID to it. Then after each poll() on given Kafka topic, it can be checked, on the consumer's side against already processed ids with the use of i.e. Hazelcast cache 
* Let's try to simulate it.
At the beginning I run docker-compose for kafka, zookeeper, hazelcast, kafka & hazelcast ui. In Java code I run polling Kafka messages method as a thread in the background

 ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/0bb7ec63-78b1-4a18-b912-3168857873ee)

 ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/97bcb5fc-9505-4cf4-b784-52697d5e4e75)

Now, after all the ids have been processed by the consumer and saved to the Hazelcast, no matter how many times I start the app no more messages of given set will be processed thanks to the business logic:

  ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/8eec8811-120a-4e2d-be98-a9ac6c0772db)
