package com.atguigu.gmall.realtime.util

/**
 * @author Hliang
 * @create 2023-07-29 21:02
 */
object MyConfig {
  val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers"
  val KAFKA_KEY_SERIALIZER = "kafka.key.serializer"
  val KAFKA_VALUE_SERIALIZER = "kafka.value.serializer"
  val KAFKA_KEY_DESERIALIZER = "kafka.key.deserializer"
  val KAFKA_VALUE_DESERIALIZER = "kafka.value.deserializer"
  val KAFKA_ENABLE_IDEMPOTENCE = "kafka.enable.idempotence"
  val KAFKA_ACKS = "kafka.acks"
  val KAFKA_ENABLE_AUTO_COMMIT = "kafka.enable.auto.commit";
  val KAFKA_AUTO_COMMIT_INTERVAL_MS = "kafka.auto.commit.interval.ms"
  val KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset"
  val REDIS_HOST = "redis.host"
  val REDIS_PORT = "redis.port"
  val ES_HOST = "es.host"
  val ES_PORT = "9200"

}
