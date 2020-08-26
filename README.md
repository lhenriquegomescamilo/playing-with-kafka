### Commands

* To start the apache kafka you will need use the following commands:

```bash
# Startup the zookeper
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```
```bash
# Startup the kafka
$ bin/kafka-server-start.sh config/server.properties       
```

### Managing the kafka

* To describe the topics
```bash
$  bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe        
```

* To change the number of partitions:
```bash
$ bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic ECOMMERCE_NEW_ORDER --partitions 3
# Warning: In this case the ECOMMERCE_NEW_ORDER is the topic 
```

* Describing the groups
```bash
$ bin/kafka-consumer-groups.sh --all-groups --bootstrap-server localhost:9092 --describe 
```


### Running the project
#### The Structure
TODO
#### Running on Docker
TODO