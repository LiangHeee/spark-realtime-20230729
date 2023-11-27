package com.atguigu.gmall.realtime.app

import java.time.{LocalDate, Period}
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.util.{MyEsUtil, MyKafkaUtil, MyOffsetsUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * 订单宽表任务
 *
 * 1.准备实时环境
 * 2.从Redis中获取Offsets * 2
 * 3.指定offsets从Kafka中消费数据 * 2
 * 4.读取该批次的最新offsets * 2
 * 5.数据处理
 *    5.1转换数据结构 * 2
 *    5.2维度关联
 *    5.3双流join
 * 6.写出到ES
 *
 * @author Hliang
 * @create 2023-08-01 21:00
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    // 1.准备实时环境
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoTopicName: String = "DWD_ORDER_INFO_I_20230729"
    val orderInfoGroupId: String = "DWD_ORDER_INFO_GROUP"

    val orderDetailTopicName: String = "DWD_ORDER_DETAIL_I_20230729"
    val orderDetailGroupId: String = "DWD_ORDER_DETAIL_GROUP"

    // 2.从Redis中获取Offsets
    val orderInfoOffsets = MyOffsetsUtil.readOffset(orderInfoGroupId, orderInfoTopicName)
    val orderDetailOffsets = MyOffsetsUtil.readOffset(orderDetailGroupId, orderDetailTopicName)

    // 3. 指定offsets从kafka中消费数据
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String,String]] = null
    if(orderInfoOffsets != null && orderInfoOffsets.nonEmpty){
      orderInfoKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroupId,orderInfoOffsets)
    }else{
      orderInfoKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc,orderInfoTopicName,orderInfoGroupId)
    }

    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String,String]] = null
    if(orderDetailOffsets != null && orderDetailOffsets.nonEmpty){
      orderDetailKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc,orderDetailTopicName,orderDetailGroupId,orderDetailOffsets)
    }else{
      orderDetailKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc,orderDetailTopicName,orderDetailGroupId)
    }

    // 4.读取数据最新的offsetRanges
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoTransformedDS = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailTransformedDS = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.数据处理
    // 5.1. 数据结构转换
    val orderInfoMappedDS = orderInfoTransformedDS.map(
      consumerRecord => {
        val orderInfoJsonStr = consumerRecord.value()
        val orderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
        orderInfo
      }
    )

    val orderDetailMappedDS = orderDetailTransformedDS.map(
      consumerRecord => {
        val orderDetailJsonStr = consumerRecord.value()
        val orderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])
        orderDetail
      }
    )

    // 5.2 orderInfo维度关联
    val orderInfoDimDS = orderInfoMappedDS.mapPartitions(
      orderInfoIter => {
        val listBuffer: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val jedis = MyRedisUtil.getJedisClientFromPool()
        val orderInfoList = orderInfoIter.toList
        for (orderInfo <- orderInfoList) {
          // 关联用户信息
          val redisUserInfoKey: String = s"DIM:USER_INFO:${orderInfo.user_id}"
          val userInfoJsonStr = jedis.get(redisUserInfoKey)
          val userInfoJsonObj = JSON.parseObject(userInfoJsonStr)
          val gender = userInfoJsonObj.getString("gender")
          val birthday = userInfoJsonObj.getString("birthday")
          val startLocalDate = LocalDate.parse(birthday)
          val endLocalDate = LocalDate.now()
          val period = Period.between(startLocalDate, endLocalDate)
          val age = period.getYears
          orderInfo.user_gender = gender
          orderInfo.user_age = age

          // 关联地区信息
          val redisAreaKey: String = s"DIM:BASE_PROVINCE:${orderInfo.province_id}"
          val areaJsonStr = jedis.get(redisAreaKey)
          val areaJsonObj = JSON.parseObject(areaJsonStr)
          val areaCode = areaJsonObj.getString("area_code")
          val areaName = areaJsonObj.getString("name")
          val areaIso31662 = areaJsonObj.getString("iso_3166_2")
          val areaIsoCode = areaJsonObj.getString("iso_code")
          orderInfo.province_area_code = areaCode
          orderInfo.province_name = areaName
          orderInfo.province_3166_2_code = areaIso31662
          orderInfo.province_iso_code = areaIsoCode

          // 处理时间
          val dateStr = orderInfo.create_time.split(" ")(0)
          val hourStr = orderInfo.create_time.split(" ")(1).split(":")(0)
          orderInfo.create_date = dateStr
          orderInfo.create_hour = hourStr

          listBuffer.append(orderInfo)
        }
        jedis.close()
        listBuffer.iterator
      }
    )

    // 5.3 双流join
    // 说到join操作，我们有一下的几种方式
    // 内连接 join  A表和B表所有能够条件匹配的都要
    // 外连接
    //     左外连接 leftOuterJoin  A表全部 + B表能够条件匹配上的
    //     右外连接 rightOuterJoin A表能够条件匹配上的 + B表全部
    //     全外连接 fullOuterJoin A表和B表的全部

    // 在我们的业务中orderInfo和orderDetail的join操作我们选择哪个？
    // 分析：一条orderInfo下面会对应多条orderDetail  并且我们要注意的是orderInfo和orderDetail一定会有匹配关系，一定能够匹配上
    // 所以根据上述分析的话，我们选择内连接join即可
    // TODO 但是我们要注意我们目前是一个实时环境，微批次的概念，会一个一个批次去拉取数据
    // TODO 因此数据延迟的问题完全可能产生 比如 批次1来了orderInfo数据，但是没有采集到orderDetail数据，
    //  而批次2中才采集到批次1中的orderInfo对应的orderDetail数据，但是join操作就会在批次1中就把没匹配的数据扔掉
    //  所以在这个情况下，批次1中的orderInfo数据就被扔掉了
    //  综上所述也就出现了丢数据的情况
    //  针对这个问题，我们首先姚解决不丢数据，所以我们只能采用全外连接fullOterJoin，无论匹配还是没匹配上，我们在每一批次中都要保留下来
    //  第二就是我们还要将相邻批次的数据存储下来，做进一步的连接匹配操作
    //  此时的方案有三种：
    //                  a、扩大采集周期（无论多大的采集周期都可能面临这个问题）  ❌
    //                  b、采用窗口（面临的问题有：窗口大小不确定、窗口中完全可能存在重复连接匹配） ❌
    //                  c、使用第三方Redis来帮助我们存储每批次中未被匹配的数据，然后再在每采集批次中引入Redis继续再一次的匹配   √
    //  综上所述，我们选择fulltOuterJoin+Redis的方案来解决数据延迟（导致的数据丢失的问题）
    val orderInfoDimMapDS = orderInfoDimDS.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailMapDS = orderDetailMappedDS.map(orderDetail => (orderDetail.order_id,orderDetail))

    val fullOurterJoinDS: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDimMapDS.fullOuterJoin(orderDetailMapDS)

    val orderWideDS = fullOurterJoinDS.mapPartitions(
      orderJoinIter => {
        val listBuffer: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis = MyRedisUtil.getJedisClientFromPool()
        val orderJoinList = orderJoinIter.toList
        for ((orderId, (orderInfoOp, orderDetailOp)) <- orderJoinList) {
          val redisOrderInfokey: String = s"ORDERJOIN:ORDER_INFO:${orderId}"
          val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderId}"
          // TODO 如果orderInfo存在、orderDetail存在
          if (orderInfoOp.isDefined) {
            if (orderDetailOp.isDefined) {
              val orderWide = OrderWide()
              val orderInfo = orderInfoOp.get
              val orderDetail = orderDetailOp.get
              orderWide.mergeOrderInfo(orderInfo)
              orderWide.mergeOrderDetail(orderDetail)
              listBuffer.append(orderWide)
            }

            // TODO 如果orderInfo存在，orderDetail不存在
            // orderInfo先写入redis
            // 分析redis结构、相关操作
            // redis采用的结构：string
            // key：ORDERJOIN:ORDER_INFO:订单ID
            // value：orderInfo的json字符串
            // 写出API：set
            // 读入API：get
            // 是否过期：24小时
            jedis.setex(redisOrderInfokey, 86400, JSON.toJSONString(OrderInfo, new SerializeConfig(true)))

            // 从redis中读取orderDetail是否存在，存在则进行关联
            val orderDetailSet: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            import scala.collection.JavaConverters._
            for (orderDetailJsonStr <- orderDetailSet.asScala) {
              val orderWide = OrderWide()
              val orderInfo = orderInfoOp.get
              val orderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])
              orderWide.mergeOrderInfo(orderInfo)
              orderWide.mergeOrderDetail(orderDetail)
              listBuffer.append(orderWide)
            }

          } else {
            // TODO 如果orderInfo不存在，orderDetail存在
            val orderDetail = orderDetailOp.get
            // 从Redis中读入orderInfo，如果有则关联
            val orderInfoJsonStr = jedis.get(redisOrderInfokey)
            if (orderInfoJsonStr != null && orderInfoJsonStr.nonEmpty) {
              val orderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
              val orderWide = OrderWide()
              orderWide.mergeOrderInfo(orderInfo)
              orderWide.mergeOrderDetail(orderDetail)
            } else {
              // 如果没有，则将orderDetail加入到redis中
              // 分析redis数据结构+操作
              // redis数据结构：set
              // key：ORDERJOIN:ORDER_DETAIL:订单ID
              // value：jsonStr、jsonStr
              // 写入API：sadd
              // 读取API：smembers
              // 是否过期：24小时
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 86400)
            }
          }
        }
        jedis.close()
        listBuffer.iterator
      }
    )

    // 6.写出到ES
    orderWideDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWideList = orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
            if(orderWideList != null && orderWideList.nonEmpty){
              val createDate = orderWideList.head._2.create_date
              val index: String = s"gmall_order_wide_${createDate}"
              MyEsUtil.bulkSave(index,orderWideList)
            }
          }
        )
        // 提交offsets
        MyOffsetsUtil.saveOffset(orderInfoGroupId,orderInfoTopicName,orderInfoOffsetRanges)
        MyOffsetsUtil.saveOffset(orderDetailGroupId,orderDetailTopicName,orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
