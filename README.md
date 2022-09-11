# kafka-throttling

JUST FOR EXPERIMENT

Create topic with partition:
`kafka-topics --create --bootstrap-server kafka:9092 --topic notification --partitions 10`

Adjust topic partition:
`kafka-topics --alter --bootstrap-server kafka:9092 --topic notification --partitions 3`

Describe consumer group:
`kafka-consumer-groups --describe --group notification-group --bootstrap-server kafka:9092`