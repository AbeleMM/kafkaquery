Here follows a description of the flow of data after starting the CodeFeedr.
The use of either the REPL or command line arguments is irrelevant for this document.<br />
<br />
Before the user can be granted access to anything, the plugins need to be set up. 
This happens on a separate occasion, before the user runs CodeFeedr.<br />
To instantiate a plugin, it needs to upload their output configuration to Zookeeper.
CodeFeedr uses Avro4s to convert a Scala case class to a JSON object and stores this object in Zookeeper.
The plugins included in CodeFeedr do so on start-up. All the user has to do is ensure that the plugins are running.
This only needs to be done once, or every time the plugin updates their configuration.
It can then start publishing data to a Kafka topic. The name of said topic is also stored in Zookeeper.<br />
<br />
When the user starts CodeFeedr, they will have to input the query that is to be processed.
This query, along with several other details, such as preferred method of output, is then saved in a configuration file.
The REPL then retrieves a list of available topics from Zookeeper and checks whether any of these topics have been requested in the query.
If so, the REPL uses Avro4s again to loop through all the fields registered on Zookeeper for a topic and registers these fields to a table.
This process will create a separate table for every requested topic.
It will then proceed to listen to the given Kafka topics.
By using FlinkSQL, the REPL is able to execute the given query on the datastream from the Kafka topic.
The result of this query can then be written to the command line, or another specified method of output.