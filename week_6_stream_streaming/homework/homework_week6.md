# DEZoomcamp-Week 6-Streaming

ferdinand.kleinschroth@gmx.de

## Which option allows Kafka to scale

- **consumer.group.id**
- replication
- consumer.commit.auto
- partitions


## Which option provide fault tolerance to kafka *
partitions
consumer.group.id
**replication**
cleanup.policy


## What is a compact topic? *
Topic which deletes messages after 7 days
Topic which compact messages based on value
**Topic which compact messages based on Key**
All topics are compact topic


## Role of schemas in Kafka *
*Making consumer producer independent of each other*
**Provide possibility to update messages without breaking change**
Allow control when producing messages
**Share message information with consumers**


## Which configuration should a producer set to provide guarantee that a message is never lost *
- ack=0
- ack=1
- **ack=all**


## From where all can a consumer start consuming messages from *
- **Beginning in topic**
- **Latest in topic**
- From a particular offset
- From a particular timestamp



## What key structure is seen when producing messages via 10 minutes Tumbling window in Kafka stream *
- Key
- [Key, Start Timestamp]
- [Key, Start Timestamp + 10 mins, Start Timestamp]
- **[Key, Start Timestamp, Start Timestamp + 10 mins]**



## Benefit of Global KTable *
- Partitions get assigned to KStream
- **Efficient joins**
- Network bandwidth reduction


## When joining KTable with KTable partitions should be *
- Different
- **Same**
- Does not matter

## When joining KStream with Global KTable partitions should be *
- Different
- Same
- **Does not matter**

## (Attach code link) Practical: Create two streams with same key and provide a solution where you are joining the two keys


## Learning in public links


## How much time (in hours) did you spend on watching lectures and reading?
- 3

## How much time (in hours) did you spend on homework?
- 2

## Did you have any problems? Do you have any comments or feedback?
- No

