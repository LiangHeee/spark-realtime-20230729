package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
 *
 * kafka的offsets管理工具类
 * 进行offsets的存储和读取
 * 主要是由于我们对InputDStream[ConsumerRecord[String,String]]还要进行下一步的转换操作
 * 所以无法直接使用其通过xxxDStream.asInstanceOf[canCommitOffsets].commitAsync(offsetRanges)获取偏移量的配置
 * （只有InputDStream才可以转换为）
 *
 * @author Hliang
* @create 2023-07-30 16:59
*/
object MyOffsetsUtil {

  /**
   * 因为offset在kafka中维护的形式是kv结构
   * k：groupid+topic+partition  （我们可以通过gtp来进行记忆）
   * v：offset值
   * @param groupId 消费者组id
   * @param topic 消费的主题
   * @param offsetRanges 所有分区及对应offset集合
   */
  def saveOffset(groupId: String,topic: String,offsetRanges: Array[OffsetRange]): Unit ={
    val jedis = MyRedisUtil.getJedisClientFromPool()

    val offsetsKey: String = s"offsets:${groupId}:${topic}"

    val offsets = new util.HashMap[String, String]()
    for(offsetRange <- offsetRanges){
      val partition = offsetRange.partition
      val untilOffset = offsetRange.untilOffset
      offsets.put(partition.toString,untilOffset.toString)
    }

    println("提交offset: " + offsets)
    jedis.hset(offsetsKey,offsets)
    jedis.close()
  }

  def readOffset(groupId: String,topic: String): Map[TopicPartition,Long] = {
    val offsets: mutable.Map[TopicPartition,Long] = mutable.Map[TopicPartition,Long]()
    val jedis = MyRedisUtil.getJedisClientFromPool()

    val offsetsKey: String = s"offsets:${groupId}:${topic}"
    val offsetRanges: util.Map[String, String] = jedis.hgetAll(offsetsKey)
    import scala.collection.JavaConverters._

    // 如果能够查到存储的offsets信息，就返回null
    if(offsetRanges != null){
      for((partition,offset) <- offsetRanges.asScala){
        val topicPartition = new TopicPartition(topic, partition.toInt)
        offsets(topicPartition) = offset.toLong
      }
    }

    jedis.close()

    offsets.toMap
  }



}
