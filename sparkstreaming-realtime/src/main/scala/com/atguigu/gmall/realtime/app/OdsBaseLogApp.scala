package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, MyOffsetsUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日志数据的消费分流
 * 1.准备实时处理环境 StreamingContext
 *
 * 2.从kafka中消费数据
 *
 * 3.处理数据
 *    3.1 转换数据结构
 *        专用结构 =》转换后的对象只适用当前的业务  bean
 *        通用结构 =》一般直接转换为JSON对象 Map
 *    3.2 分流
 *
 * 4.写出到dwd层
 *
 * @author Hliang
 * @create 2023-07-29 21:22
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 1.创建StreamingContext环境对象
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base_log_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 2.从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG_20230729"
    val groupId: String = "ODS_BASE_LOG_GROUP"
    // TODO 指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtil.readOffset(groupId, topicName)

    // 一定要判断是否null的问题，因为Redis首次启动是没有存储offsets的信息的
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc,topicName,groupId,offsets)
    }else{
      // TODO ConsumerRecord是没有进行序列化的，不能直接print
      // TODO 因为Direct API中，是由计算的Executor主动去拉取消费数据
      // TODO 但是最后print操作，需要回到Driver端进行数据的打印，有需要将ConsumerRecord传递回Driver端，所以ConsumerRecord我们要求必须序列化
      // TODO 此处ConsumerRecord没有办法序列化，所以就报没有序列化的错误了
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId)
    }

    var offsetRanges: Array[OffsetRange] = null
    val transformedDS: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 3.处理数据
    // TODO 转换为通用结构，JsonObject是进行了序列化的，可以传递到Driver端最终打印输出了，就不会再报错
    val mappedDS: DStream[fastjson.JSONObject] = transformedDS.map(
      consumerRecord => {
        val jsonStr = consumerRecord.value()
        val jsonObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    )

//    mappedDS.print(100)

    // 4.开始分流
    // 用户行为日志主要分为页面数据和启动数据
    // 页面数据
    //     公共字段
    //    页面浏览数据
    //    曝光数据
    //    事件数据（行为数据）
    //    错误数据
    // 启动数据
    //    启动数据
    //    错误数据
    val DWD_PAEG_LOG_TOPIC = "DWD_PAGE_LOG_20230729"
    val DWD_PAGE_DISPALY_LOG_TOPIC = "DWD_PAGE_DISPALY_LOG_20230729"
    val DWD_PAGE_ACTION_LOG_TOPIC = "DWD_PAGE_ACTION_LOG_20230729"
    val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_20230729"
    val DWD_START_LOG_TOPIC = "DWD_START_LOG_20230729"

    mappedDS.foreachRDD(
      rdd => {
        // TODO 一个一个分区数据进行一次flush刷写，比foreach单独一条一条数据刷写效率会更高（这个一条一条相当于同步发送数据了）
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              // 4.1 如果是错误日志，直接将整个错误信息转换为jsonStr写出到对应的主题
              val errorObj = jsonObj.getJSONObject("err")
              if(errorObj != null){
                MyKafkaUtil.send(DWD_ERROR_LOG_TOPIC,errorObj.toJSONString)
              }else{
                // 4.2 进行其他数据的分流
                // 4.2.1 首先提取公共字段
                val commonObj = jsonObj.getJSONObject("common")
                val provinceId = commonObj.getString("ar")
                val userId = commonObj.getString("uid")
                val operateSystem = commonObj.getString("os")
                val channel = commonObj.getString("ch")
                val isNew = commonObj.getString("is_new")
                val model = commonObj.getString("md")
                val mid = commonObj.getString("mid")
                val versionCode = commonObj.getString("vc")
                val brand = commonObj.getString("ba")

                val ts = jsonObj.getString("ts").toLong

                // 4.2.1 如果是页面数据
                val pageObj = jsonObj.getJSONObject("page")
                if(pageObj != null){
                  val pageId = pageObj.getString("page_id")
                  val pageItem = pageObj.getString("item")
                  val duringTime = pageObj.getString("during_time").toLong
                  val pageItemType = pageObj.getString("item_type")
                  val lastPageId = pageObj.getString("last_page_id")
                  val sourceType = pageObj.getString("source_type")

                  // 封装成为专用对象转换为jsonStr发送到相应主题
                  val pageLog =
                    PageLog(mid,userId,provinceId,channel,isNew,model,operateSystem,versionCode,brand,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,ts)
                  MyKafkaUtil.send(DWD_PAEG_LOG_TOPIC,JSON.toJSONString(pageLog,new SerializeConfig(true)))

                  // 4.2.1.1 判断是否包含曝光数据
                  val displayArr = jsonObj.getJSONArray("displays")
                  if(displayArr != null && displayArr.size() > 0){
                    for(i <- 0 until displayArr.size()){
                      val displayObj = displayArr.getJSONObject(i)
                      val displayType = displayObj.getString("display_type")
                      val displayItem = displayObj.getString("item")
                      val displayItemType = displayObj.getString("item_type")
                      val displayPosId = displayObj.getString("pos_id").toLong
                      val displayOrder = displayObj.getString("order").toLong
                      val pageDisplayLog =
                        PageDisplayLog(mid,userId,provinceId,channel,isNew,model,operateSystem,versionCode,brand,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,displayType,displayItem,displayItemType,displayOrder,displayPosId,ts)
                      MyKafkaUtil.send(DWD_PAGE_DISPALY_LOG_TOPIC,JSON.toJSONString(pageDisplayLog,new SerializeConfig(true)))
                    }
                  }

                  // 4.2.1.2 判断是否包含行为数据
                  val actionArr = jsonObj.getJSONArray("actions")
                  if(actionArr != null && actionArr.size() > 0){
                    for(i <- 0 until actionArr.size()){
                      val actionObj = actionArr.getJSONObject(i)
                      val actionItem = actionObj.getString("item")
                      val actionId = actionObj.getString("action_id")
                      val actionItemType = actionObj.getString("item_type")
                      val actionTs = actionObj.getString("ts").toLong
                      val pageActionLog = PageActionLog(mid,userId,provinceId,channel,isNew,model,operateSystem,versionCode,brand,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,actionId,actionItem,actionItemType,actionTs,ts)
                      MyKafkaUtil.send(DWD_PAGE_ACTION_LOG_TOPIC,JSON.toJSONString(pageActionLog,new SerializeConfig(true)))
                    }
                  }
                }

                // 4.2.2 如果是启动数据
                val startObj = jsonObj.getJSONObject("start")
                if(startObj != null){
                  val entry = startObj.getString("entry")
                  val openAdSkipMs = startObj.getString("open_ad_skip_ms").toLong
                  val openAdMs = startObj.getString("open_ad_ms").toLong
                  val loadingTime = startObj.getString("loading_time").toLong
                  val openAdId = startObj.getString("open_ad_id").toLong
                  val startLog = StartLog(mid,userId,provinceId,channel,isNew,model,operateSystem,versionCode,brand,entry,openAdId,loadingTime,openAdMs,openAdSkipMs,ts)
                  MyKafkaUtil.send(DWD_START_LOG_TOPIC,JSON.toJSONString(startLog,new SerializeConfig(true)))
                }
              }
            }
            // TODO 遍历完一个分区数据后进行flush
            MyKafkaUtil.flush()
          }
        )
//        rdd.foreach(
//          jsonObj => {
//          }
//        )
        // TODO 在这个位置进行offset的手动提交，因为这里处理Driver端执行，针对每个批次的数据进行周期性的操作
        // TODO 后置提交offset解决了漏数据的问题，但是仍然存在数据重复的问题
        // TODO 数据重复我们通过外部组件ElasticSearch本身就有过滤重复数据的特性来实现
        MyOffsetsUtil.saveOffset(groupId,topicName,offsetRanges)
      }
    )



    ssc.start()
    ssc.awaitTermination()
  }

}
