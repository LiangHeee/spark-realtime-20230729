package com.atguigu.gmall.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable.ListBuffer

/**
 * ES的写入操作和读操作
 *
 * @author Hliang
 * @create 2023-08-06 11:32
 */
object MyEsUtil {

  val client: RestHighLevelClient = createRestHighLevelClient

  def createRestHighLevelClient(): RestHighLevelClient = {
    val restClientBuilder = RestClient.builder(new HttpHost(MyPropertiesUtil(MyConfig.ES_HOST), MyConfig.ES_PORT.toInt))
    val restHighLevelClient = new RestHighLevelClient(restClientBuilder)
    restHighLevelClient
  }

  def closeRestHighLevelClient(): Unit ={
    if(client != null){
      client.close()
    }
  }

  /**
   * 批量写入ES，并且要使用幂等方式用于去重
   * @param index 索引名称
   * @param docs 文档集合（docId，docData）
   */
  def bulkSave(index: String,docs: List[(String,AnyRef)]): Unit ={
    val bulkRequest = new BulkRequest(index)
    for ((docId,docData) <- docs) {
      val indexRequest = new IndexRequest()
      val dataJsonStr = JSON.toJSONString(docData, new SerializeConfig(true))
      indexRequest.id(docId)
      indexRequest.source(dataJsonStr,XContentType.JSON)
      bulkRequest.add(indexRequest)
    }
    client.bulk(bulkRequest,RequestOptions.DEFAULT)
  }

  /**
   * 指定读取某个字段的值
   * @param index 索引名
   * @param fieldName 字段名
   * @return
   */
  def searchField(index: String,fieldName: String): List[String] = {
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest = new SearchRequest(index)
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName,null)
    // 设置查询结果窗口大小
    searchSourceBuilder.size(100000)
    searchRequest.source(searchSourceBuilder)
    // 提交api拿到查询结果
    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val map = hit.getSourceAsMap
      val mid: String = map.get(fieldName).toString
      mids.append(mid)
    }

    mids.toList
  }

  def main(args: Array[String]): Unit = {
    val mids = searchField("gmall_dau_info_2023-08-06", "mid")
    println(mids)
  }

}
