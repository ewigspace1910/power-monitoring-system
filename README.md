# power-monitoring-system
Simple project with Kafka, Spark streaming, PowerBI

## Data
Detailed household load in minutely resolution in [here](https://data.open-power-system-data.org/household_data/)

We reduce some features, then schema like: 
| timestemp | pv |

## Init Kafka

	1. Config
	2. Producer

## Spark Streaming App
	1. Read data
	2. Aggregate
	3. Write stream

## Cassandra

## Dashboard
Finaly, we using PowerBI to visualize and monitoring the streamings.
