package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}
import com.atguigu.gmall.realtime.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.ElasticsearchStatusException
import redis.clients.jedis.Pipeline

import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 *
 * 1.准备实时环境
 * 2.从Redis中读取offsets
 * 3.从kafka中消费数据
 * 4.提取最新消费的offsets
 * 5.处理数据
 *    5.1 转换数据结构
 *    5.2 去重 （自我审查和第三方审查）
 *    5.3 维度关联
 * 6.写入ES
 * 7.手动提交offsets
 *
 * @author Hliang
 * @create 2023-08-01 11:28
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {

    // 恢复状态
    resetStatus()

    // 1.准备实时环境
    val sprakConf = new SparkConf().setMaster("local[4]").setAppName("dwd_dau_app")
    val ssc = new StreamingContext(sprakConf, Seconds(5))

    val topicName: String = "DWD_PAGE_LOG_20230729"
    val groupId: String = "DWD_DAU_GROUP"

    // 2.从redis中获取offsets
    val offsets = MyOffsetsUtil.readOffset(groupId, topicName)

    // 3.从kafka指定offsets消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String,String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc,topicName,groupId)
    }

    // 4.获取该批次消费数据的offsets
    var offsetRanges: Array[OffsetRange] = null
    val transformedDS = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.处理数据
    // 5.1 转换数据结构
    val mappedDS: DStream[PageLog] = transformedDS.map(
      consumerRecord => {
        val jsonStr = consumerRecord.value()
        val pageLog = JSON.parseObject(jsonStr, classOf[PageLog])
        pageLog
      }
    )

    // 5.2 去重
    // 5.2.1 自我审查
    val filterDS: DStream[PageLog] = mappedDS.filter(
      pageLog => pageLog.last_page_id == null
    )

    // 5.2.2 第三方审查
    //  filterDS.filter() 用这个来做第三方审查不太好，因为他会对每一个rdd的数据进行一次第三方审查，
    //  就会频繁获取关闭redis连接（redis连接只有写在内部，TODO 因为连接都是不可以序列化的）
    val redisFilterDS: DStream[PageLog] = filterDS.mapPartitions(
      pageLogIter => {
        val pageLogList = pageLogIter.toList
        val listBuffer = ListBuffer[PageLog]()
        val jedis = MyRedisUtil.getJedisClientFromPool()
        // 获取redis中存储的key
        // 使用的数据结构 list set
        // key的值  DIM:DATE
        // value的值 mid
        // 写入API lpush|rpush   sadd
        // 读取API lrange        smembers
        // 是否过期 24小时过期
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        var dateStr: String = null
        if(pageLogList != null && pageLogList.nonEmpty ){
          dateStr = sdf.format(new Date(pageLogList.head.ts))
        }
        val redisDauMidKey = s"DAU:${dateStr}"
        for (pageLog <- pageLogList) {
          // TODO 无论使用哪种数据格式，在目前的写法下都会存在并发问题
          // TODO 因为我们目前是分布式执行的，完全可能多个cpu对于同一个mid同时通过了if判断，那么listBuffer就会加入多次同样的pageLog数据，就没达到去重的目的
          // 如果当前使用的是List
          //          val midList = jedis.lrange(redisDauMidKey, 0, -1)
          //          if(!midList.contains(pageLog.mid)){
          //            listBuffer.append(pageLog)
          //          }

          // 如果当前使用的是set
          //          val midSet = jedis.smembers(redisDauMidKey)
          //          if(!midSet.contains(pageLog.mid)){
          //            listBuffer.append(pageLog)
          //          }

          // TODO 注意sadd操作会有返回值0或者1，0表示插入失败，1表示插入成功
          // TODO 下面的操作就能够解决并发的问题了，因为redis是单线程的，即使有多台机器同时发送了针对同一个mid的sadd请求，
          // TODO 但是单线程也会挨着执行，所以最后返回的isNew肯定也是会有区别0L和1L的
          val isNew: Long = jedis.sadd(redisDauMidKey, pageLog.mid)
          if (isNew == 1L) {
            listBuffer.append(pageLog)
          }
        }
        jedis.expire(redisDauMidKey,86400) // 86400秒，也就是24小时
        jedis.close()
        listBuffer.iterator
      }
    )

//    redisFilterDS.print(100)

    // 5.3 维度关联
    val dauInfoDS: DStream[DauInfo] = redisFilterDS.mapPartitions(
      pageLogIter => {
        val pageLoglist = pageLogIter.toList
        val listBuffer = ListBuffer[DauInfo]()
        val jedis = MyRedisUtil.getJedisClientFromPool()
        val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
        val sdfHour = new SimpleDateFormat("HH")
        for (pageLog <- pageLoglist) {
          // 创建DuaInfo对象，用于维度关联
          val dauInfo = new DauInfo()
          MyBeanUtil.copyProperties(pageLog, dauInfo)
          // 获取维度信息
          //用户性别 年龄
          val userId = pageLog.user_id
          val redisUserIdKey: String = s"DIM:USER_INFO:${userId}"
          val userInfoJsonStr = jedis.get(redisUserIdKey)
          val userInfoJsonObj = JSON.parseObject(userInfoJsonStr)
          val gender = userInfoJsonObj.getString("gender")
          val birthday = userInfoJsonObj.getString("birthday")
          val endLocalDate = LocalDate.now()
          val startLocalDate: LocalDate = LocalDate.parse(birthday)
          val period: Period = Period.between(startLocalDate, endLocalDate)
          val age = period.getYears
          // 向DauInfo对象中设置gender和age
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //地区信息
          val provinceId = pageLog.province_id
          val redisProvinceKey: String = s"DIM:USER_INFO:${provinceId}"
          val provinceJsonStr = jedis.get(redisProvinceKey)
          val provinceJsonObj = JSON.parseObject(provinceJsonStr)
          val areaCode = provinceJsonObj.getString("area_code")
          val areaName = provinceJsonObj.getString("name")
          val iso31662 = provinceJsonObj.getString("iso_3166_2")
          val isoCode = provinceJsonObj.getString("iso_code")
          // 向DauInfo对象中设置地区信息
          dauInfo.province_area_code = areaCode
          dauInfo.province_name = areaName
          dauInfo.province_3166_2 = iso31662
          dauInfo.province_iso_code = isoCode

          //日期
          val date = new Date(pageLog.ts)

          val dateStr = sdfDate.format(date)
          val hourStr = sdfHour.format(date)

          dauInfo.dt = dateStr
          dauInfo.hr = hourStr

          listBuffer.append(dauInfo)
        }
        jedis.close()
        listBuffer.iterator
      }
    )

    // 6.写出到ES
    // 按照天分割索引 =》 创建索引模板
    // 为了便于周期性查看某个周期的数据 =》 创建索引别名
    dauInfoDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val dauInfoList: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            if(dauInfoList != null && dauInfoList.nonEmpty){
              // 索引名中的时间说明
              // 如果是在实际开发中，我们直接用系统时间即可
              // 但是目前我们是在练习中，生成的模拟数据，所以时间要用数据中的时间
              val dauInfoHead = dauInfoList.head
              val dateStr = dauInfoHead._2.dt
              val index: String = s"gmall_dau_info_${dateStr}"
              MyEsUtil.bulkSave(index,dauInfoList)
            }
          }
        )
        // 手动提交offset
        MyOffsetsUtil.saveOffset(groupId,topicName,offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 为了解决redis中mid写入成功，但是往ES中mid写入失败情况，我们进行装填恢复
   * 装填恢复的流程如下：
   * 1、清空redis中所有的mid
   * 2、将当前ES中所有的mid同步到redis中
   */
  def resetStatus(): Unit ={
    val jedis = MyRedisUtil.getJedisClientFromPool()
    /**
     * TODO 我们限制每天生产的数据必须是当天的数据
     */
    val date = LocalDate.now()
    val redisDauMidKey = s"DAU:$date"
    jedis.del(redisDauMidKey)

    // 从ES中获取mid数据
    val index: String = s"gmall_dau_info_$date"
    var mids: List[String] = null
    try {
      // 当天可能没有数据产生
      mids = MyEsUtil.searchField(index,"mid")
    }catch{
      case ex => println(ex)
    }

    if(mids != null && mids.nonEmpty){
      //      for (mid <- mids) {
      //        jedis.sadd(redisDauMidKey,mid)
      //      }
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        // 把jedis需要操作的数据添加到管道中，之后批量一次性同步到redis中
        pipeline.sadd(redisDauMidKey,mid)
      }

      // pipeline同步数据道redis中执行（进行批量添加）
      pipeline.sync()
    }

    jedis.close()
  }
}
