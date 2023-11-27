package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, MyOffsetsUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 业务数据的分流操作
 * 1.准备好实时环境ssc
 *
 * 2.获取redis中的offset
 *
 * 3.从kafka指定的offset消费数据
 *
 * 4.获取该批次消费的offset
 *
 * 5.数据转换
 *      获取kafka中的ConsumerRecord的value，转换为jsonObj
 *
 * 6.分流处理
 *      事实表数据，SparkStreaming分流处理后写进kafka（以表为基准，以操作为单位）
 *      维度表数据，SparkStreaming分流处理后写进Redis
 *
 * 7.提交offset至redis
 *
 * @author Hliang
 * @create 2023-07-31 12:18
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    // 创建实时环境
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base_db_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "ODS_BASE_DB_20230729"
    val groupId: String = "ODS_BASE_DB_GROUP"

    // 从redis中获取kafka消费的offsets
    val offsets = MyOffsetsUtil.readOffset(groupId, topicName)

    // 从指定的offsets开始消费
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc,topicName,groupId)
    }

    // 获取该批次消费的offsets
    var offsetRanges: Array[OffsetRange] = null
    val transformedDS = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )


    // 转换数据结构
    val mappedDS: DStream[JSONObject] = transformedDS.map(
      consumerRecord => {
        val jsonStr = consumerRecord.value()
        val jsonObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    )

    //    mappedDS.print(100)

    // 定义事实表有哪些 TODO 不能动态配置表清单
//    val dwdTables = List("order_info","order_detail")
    // 定义维度表有哪些
//    val dimTables = List("user_info","base_province")
    val factTablesKey: String = "FACT:TABLES"
    val dimTablesKey: String = "DIM:TABLES"

    // 分流处理
    mappedDS.foreachRDD(
      rdd => {
        val outerJedis = MyRedisUtil.getJedisClientFromPool()
        // TODO 以分区为单位动态获取事实表清单和维度表清单
        val factTables: util.Set[String] = outerJedis.smembers(factTablesKey)
        val dimTables: util.Set[String] = outerJedis.smembers(dimTablesKey)
//        println(factTables)
//        println(dimTables)
        // 使用广播变量，减少IO次数和内存占用，使得一个Executor节点中所有的Task共享同一份数据
        val factTablesBC = ssc.sparkContext.broadcast(factTables)
        val dimTablesBC = ssc.sparkContext.broadcast(dimTables)

        outerJedis.close()

        rdd.foreachPartition(
          jsonObjIter => {
            // TODO 以分区为单位获取jedis连接，
            //  在这里获取jedis连接，解决executor每执行一次就要获取和关闭jedis连接的问题，数据量大的情况下频繁获取关闭连接性能消耗很大
            val jedis = MyRedisUtil.getJedisClientFromPool()
            val jsonObjList = jsonObjIter.toList
            for (jsonObj <- jsonObjList) {
              // 获取日志操作类型，再进行下一步的处理
              // maxwell产生的json数据中会有type字段，可以获取操作的类型
              // 操作的类型可以拿来识别不同操作进行下一步处理，还可以用于过滤不必要的数据
              val dataType = jsonObj.getString("type")
              val dataTypeMatch = dataType match {
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case "bootstrap-insert" => "I"  // 用于全量同步时过滤数据
                case _ => null
              }

              // 如果数据类型满足要求，则进行下一步的分流
              if(dataTypeMatch != null){
                // 获取当前数据所操作的表
                val table = jsonObj.getString("table")
                // 如果当前数据是事实表数据，则写入kafka的对应操作主题
                if(factTablesBC.value.contains(table)){
                  val topic: String = s"DWD_${table.toUpperCase}_${dataTypeMatch}_20230729"
                  val msg = jsonObj.getString("data")
                  MyKafkaUtil.send(topic,msg)
                }

                // 如果当前数据是维度表数据，则写入Redis
                if(dimTablesBC.value.contains(table)){
                  // TODO Redis不能使用hash的数据结构
                  // 情况1：假设使用hash肯定是key为表名，其中的field为主键id，value为一行的数据
                  //      问题：redis单节点的存储上限问题，无论我们怎么扩展单节点redis的硬盘，也无法面对大数据场景中数据量大的问题，一张表完全可能数据特别大
                  //           由于我们这种存储方案，以表名为key，那么一个hash只能存入一个redis节点，从而也无法进行Redis的横向扩容，也就是采用集群模式是无法解决数据量大的问题的
                  // 情况2：hash是key为主键id，其中的field为字段名，value是对应的值
                  //      问题：在我们的业务场景中，存储维度数据，暂时不用将字段拆开，这样在数据量大的情况下，其实也很耗费性能，
                  //      但是其解决了情况1中的key的slot单独一个的问题，可以采用redis横向扩展解决硬盘不足问题

                  // TODO 所以redis中我们用String数据结构来存储
                  // 其中key是DIM:表名:主键id    value是对应这行的json数据
                  val innerDataJsonObj = jsonObj.getJSONObject("data")
                  val id: Long = innerDataJsonObj.getLong("id")
                  val redisKey: String = s"DIM:${table.toUpperCase}:${id}"
                  // TODO 每处理一条数据，就获取jedis连接，性能消耗很大
//                  val jedis = MyRedisUtil.getJedisClientFromPool()
                  jedis.set(redisKey,innerDataJsonObj.toJSONString)
                  // TODO 频繁关闭jedis连接，性能消耗很大
//                  jedis.close()
                }
              }
            }
            // TODO 每一个分区遍历结束后关闭jedis连接
            jedis.close()
            // 手动flush数据到kafka
            MyKafkaUtil.flush()
          }
        )
        // 手动提交偏移量offsets
        MyOffsetsUtil.saveOffset(groupId,topicName,offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

}
