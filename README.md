<h1>Kafka at-least-once problem - my lab</h1>
* Sometimes the acknowledgement of kafka message being processed by @KafkaListener may not be delivered to the broker (i.e. temporary connection problem). In the end after the timeout specified it gets re-sent by the producer. This way at-least-once delivery may happen. It can be simulated. For example I could create an app that based on its context loaded is sending a certain, prepared set of data that can be send multiple times if the app is being re started over and over again.
* There is a solution - in-memory cache (as external service i.e. running on Docker)
* We can save the processed messages ID to it. Then after each poll() on given Kafka topic, it can be checked, on the consumer's side against already processed ids with the use of i.e. Hazelcast cache 
* Let's try to simulate it.
At the beginning I run docker-compose for kafka, zookeeper, hazelcast, kafka & hazelcast ui. In Java code I run polling Kafka messages method as a thread in the background

 ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/0bb7ec63-78b1-4a18-b912-3168857873ee)

 ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/97bcb5fc-9505-4cf4-b784-52697d5e4e75)

Now, after all the ids have been processed by the consumer and saved to the Hazelcast, no matter how many times I start the app no more messages of given set will be processed thanks to the business logic:

  ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/8eec8811-120a-4e2d-be98-a9ac6c0772db)

<h1>Kafka - after rebalancing offset gets lost - my lab</h1>
* Now let's test how Kafka offset controlling mechanisms work. First step is to disable "auto.commit" property of producer and mock Hazelcast cache to let app process same messages over and over again. Plus I cleared some logs not connected with this lab and set consumer's props for "fetch.min.bytes/fetchmax.wait.ms.config" for the same purpose (every invocation of poll() method leaves some logs bluring it a little bit)
* Offset can be controlled manually by sync/async committing while method poll() fetching Kafka records occurs or by listening to rebalnce happening (method onPartitionsRevoked()).
* To check what offset Kafka is on, easy way we can start app second time and read the logs, or just edit IJ configuration and start next isntance of an app while -- trigger it manually.
* So the offset gets set by saving it in distributed Hazelcast cache and used on invocation of method:
onPartitionsAssigned(Collection<TopicPartition> partitions)
* After restart of an app in a minute:

  ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/4bf5c152-57af-4e7d-aab3-4aa82fea7005)

* fun fact: even after overriding Kafka rebalance listener in onPartitionsAssigned(Collection<TopicPartition> partitions) method and commenting seek() method out, which is setting offset for a TopicPartition (trying to destroy app a little bit:)) the Kafka consumer's group coordinator comes into place to do the job and save the situation (prevent reading same data over and over). So as long as we keep committing often enough, there is no need to worry about rebalancing
  ![obraz](https://github.com/tomeee121/kafka-safe-consumer/assets/85828070/6c78b748-ef58-43b9-a978-bb780aee83e0)


  
