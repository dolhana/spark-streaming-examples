Spark Streaming with Cassandra Example
======================================

Overview
--------

This example started from [Spark Streaming Programming Guide][spark-streaming-guide] and added [Spark Cassandra Connector][spark-cassandra-connector] to use Cassandra as a persistent storage.

[spark-streaming-guide]: http://spark.apache.org/docs/latest/streaming-programming-guide.html
[spark-cassandra-connector]: https://github.com/datastax/spark-cassandra-connector 

Preparation
-----------

### Installing Docker

Follow the instructions [here](http://docs.docker.com/mac/started/).

### Installing Cassandra Docker

Pull Cassandra Docker image from [Docker Registry](https://registry.hub.docker.com/_/cassandra/).

    $ docker pull cassandra:2.1

Start a Cassandra Docker container.

    $ docker run --name cassandra-01 -d -e CASSANDRA_BROADCAST_ADDRESS=$(boot2docker ip) -p 7000:7000 -p 9042:9042 cassandra:2.1

Check the container is working.

    $ docker run -it --link cassandra-01:cassandra --rm cassandra:2.1 cqlsh cassandra

Running
-------

This Spark application connects to nc listening at tcp:localhost:9999 and receives any text typed in to the nc.

So, first, you need to run nc listening at 9999 on localhost.

    $ nc -lk 9999

Then run this example in your preferred way. It's just a plain Java application and starts Spark cluster on the local host. I've used Eclipse Debug.

If you want to see the result in Cassandra, run the following queries in cqlsh.

    $ docker run -it --link cassandra-01:cassandra --rm cassandra:2.1 cqlsh cassandra
    cqlsh> USE test_keyspace;
    cqlsh:test_keyspace> select * from word_count;

     word  | count
    -------+-------
     hello |     3
     world |     3

The word counts are accumulated in the table.
