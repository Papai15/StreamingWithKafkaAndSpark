trait AppConfigs {

  @transient lazy val BootStrapServers = "localhost:9092"
  @transient lazy val ApplicationID = "CustomProducer"
  @transient lazy val Topic = "toCassandra"
  @transient lazy val AOR = "earliest"
  @transient lazy val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"
  @transient lazy val MASTER_MODE = "local[2]"
  @transient lazy val TABLE = "cellphones"
  @transient lazy val KEYSPACE = "test"
  @transient lazy val CHECK_POINT_DIR = "B:/tmp"
  @transient lazy val CASSANDRA_HOST = "127.0.0.1"
  @transient lazy val CASSANDRA_PORT = "9042"

}
