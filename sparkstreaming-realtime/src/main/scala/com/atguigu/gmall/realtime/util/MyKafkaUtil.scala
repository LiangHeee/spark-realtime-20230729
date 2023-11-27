package com.atguigu.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * @author Hliang
 * @create 2023-07-29 18:22
 */
object MyKafkaUtil {

  /**
   * 定义kafka消费者相关配置
   * 1.连接kafka集群地址
   * 2.key和value的反序列化器
   * 3.消费者组id
   * 4.offsets是自动提交还是手动提交
   * 5.offsets是自动提交的话，offsets提交周期是多少（默认是5s）
   * 6.offsets重置问题：earliest还是latest
   */
  val kafkaParams: mutable.Map[String,Object] = mutable.Map[String,Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropertiesUtil(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> MyPropertiesUtil(MyConfig.KAFKA_KEY_DESERIALIZER),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyPropertiesUtil(MyConfig.KAFKA_VALUE_DESERIALIZER),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> MyPropertiesUtil(MyConfig.KAFKA_ENABLE_AUTO_COMMIT),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> MyPropertiesUtil(MyConfig.KAFKA_AUTO_OFFSET_RESET)
  )

  /**
   * 定义SparkStreaming消费数据通用方法-不能指定offset版本
   */
  def getKafkaDStream(ssc: StreamingContext,topic: String,groupId: String) = {
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)

    val kafkaDStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParams))
    kafkaDStream
  }

  /**
   * 定义SparkStreaming消费数据通用方法-指定offset版本
   */
  def getKafkaDStream(ssc: StreamingContext,topic: String,groupId: String,offsets: Map[TopicPartition,Long]) = {
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)

    val kafkaDStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParams,offsets))
    kafkaDStream
  }



  /**
   * 消费者属性，模拟单例模式
   */
  val kafkaProducer: KafkaProducer[String,String] = createKafkaProducer()


  /**
   * 创建kafka生产者
   */
  def createKafkaProducer(): KafkaProducer[String,String] = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropertiesUtil(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,MyPropertiesUtil(MyConfig.KAFKA_KEY_SERIALIZER))
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,MyPropertiesUtil(MyConfig.KAFKA_VALUE_SERIALIZER))
    // memory.size 32M
    // batch.size 16KB
    // linger.ms 0s
    // retries 0
    // enable.idempotence false 将幂等性打开，置为true
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,MyPropertiesUtil(MyConfig.KAFKA_ENABLE_IDEMPOTENCE))
    properties.put(ProducerConfig.ACKS_CONFIG,MyPropertiesUtil(MyConfig.KAFKA_ACKS))
    new KafkaProducer[String,String](properties)
  }

  /**
   * 发送数据
   */
  def send(topic: String,msg: String): Unit = {
    if(kafkaProducer != null){
      kafkaProducer.send(new ProducerRecord[String,String](topic,msg))
    }
  }

  /**
   * 刷写缓冲区到Broker节点磁盘中
   */
  def flush() = {
    if(kafkaProducer != null){
      kafkaProducer.flush()
    }
  }

  /**
   * 关闭kafkaProducer对象
   */
  def closeKafkaProducer(): Unit ={
    if(kafkaProducer != null){
      kafkaProducer.close()
    }
  }


}
