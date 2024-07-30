Notes Regarding Streaming Part:

1-There was a trade-off between writing only one stream with a common schema and paying the cost of storing nulls due to data production, 
	versus writing more than one stream with dynamic schemas using the filter transformation in Spark, 
	which incurs the cost of handling multiple streams. In a real-life deployment, this might be resolved by using a schema registry and sending Avro data.

1-Stateful streaming jobs write to a PostgreSQL database to gain experience with different data sources. However, multiple problems appeared,
	such as the fact that Parquet and PostgreSQL don't support the "Update" output mode.
	The approach implemented aimed to gain experience dealing with "managed window streaming."
	Here is a JIRA ticket for the "Update" feature that the community requested to allow writing streams with update mode to databases:
	https://issues.apache.org/jira/browse/SPARK-19335
