package com.atguigu.es

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.apache.lucene.search.TotalHits
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedStringTerms, Terms}
import org.elasticsearch.search.aggregations.metrics.ParsedAvg
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

/**
 * @author Hliang
 * @create 2023-08-04 20:48
 */
object EsTest {
  def main(args: Array[String]): Unit = {
//    putIdepmpotence()
//    put()
//    bulk()
//    update()
//    updateByQuery()
//    delete()
//    getById()
//    searchByCondition()
    searchAgg()
    closeRestHighLevelClient()
  }

  val client: RestHighLevelClient = createRestHighLevelClient()

  def createRestHighLevelClient(): RestHighLevelClient = {
    val restClient = RestClient.builder(
      new HttpHost("hadoop102", 9200), new HttpHost("hadoop103", 9200), new HttpHost("hadoop104", 9200))
    val restHighLevelClient = new RestHighLevelClient(restClient)
    restHighLevelClient
  }

  def closeRestHighLevelClient(): Unit = {
    client.close()
  }

  /**
   * 幂等写入-指定id
   */
  def putIdepmpotence(): Unit = {
    val indexRequest = new IndexRequest("movie_index_cn2")
    val movie = Movie(1L,"红海行动")
    val jsonStr = JSON.toJSONString(movie,new SerializeConfig(true))
    indexRequest.source(jsonStr,XContentType.JSON)

    // 指定id就是幂等写入
    indexRequest.id("1001")
    client.index(indexRequest,RequestOptions.DEFAULT)
  }

  /**
   * 非幂等写入-不指定id
   */
  def put(): Unit = {
    val indexRequest = new IndexRequest("movie_index_cn2")
    val movie = Movie(2L, "红海事件")
    val jsonStr = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(jsonStr,XContentType.JSON)
    // 不指定id，则表示非幂等写入
    client.index(indexRequest,RequestOptions.DEFAULT)
  }

  /**
   * 批量操作，可以进行批量添加、批量删除、批量更新
   * 批量非幂等操作，如果幂等，则指定id即可
   * @return
   */
  def bulk() = {
    val bulkRequest = new BulkRequest()
    val movies = List(Movie(3L, "八佰"), Movie(4L, "战狼"), Movie(5L, "狙击手"))
    for (movie <- movies) {
      val jsonStr = JSON.toJSONString(movie,new SerializeConfig(true))
      val indexRequest = new IndexRequest("movie_index_cn2")
      indexRequest.source(jsonStr,XContentType.JSON)
//      indexRequest.id("5000L")
      bulkRequest.add(indexRequest)
    }
    client.bulk(bulkRequest,RequestOptions.DEFAULT)
  }

  /**
   * TODO 根据docid更新字段
   */
  def update(): Unit ={
    val updateRequest = new UpdateRequest("movie_index_cn2", "1001")
    val map = new util.HashMap[String, AnyRef]()
    map.put("name","红海行动")
    updateRequest.doc(map)
    client.update(updateRequest,RequestOptions.DEFAULT)
  }

  /**
   * 根据查询结果进行修改
   */
  def updateByQuery(): Unit = {
    // 创建更新请求
    val updateByQueryRequest = new UpdateByQueryRequest("movie_index_cn2")
    // 创建查询匹配条件
    val boolQuery = QueryBuilders.boolQuery()
    val termQuery = QueryBuilders.termQuery("name.keyword", "红海行动")
    boolQuery.filter(termQuery)
    updateByQueryRequest.setQuery(boolQuery)
    // 创建修改的脚本Script
    val map = new util.HashMap[String, AnyRef]()
    map.put("newName","红海行动1")
    val script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,"ctx._source['name']=params.newName", map)
    updateByQueryRequest.setScript(script)
    client.updateByQuery(updateByQueryRequest,RequestOptions.DEFAULT)
  }

  /**
   * 删除数据
   */
  def delete(): Unit ={
    val deleteRequest = new DeleteRequest("movie_index_cn2")
    deleteRequest.id("XWQixYkB0_bPwXuwMriv")
    client.delete(deleteRequest,RequestOptions.DEFAULT)
  }

  /**
   * 根据id查询单条数据
   */
  def getById(): Unit ={
    val getRequest = new GetRequest("movie_index_cn2","1001")
    val getResponse: GetResponse = client.get(getRequest, RequestOptions.DEFAULT)
    val source: util.Map[String, AnyRef] = getResponse.getSource
    val name = source.get("name")
    val id = source.get("id")
    println(s"name=${name},id=${id}")
  }

  /**
   * 条件查询
   * search :
   * 查询 doubanScore>=5.0 关键词搜索 red sea
   * 关键词高亮显示
   * 显示第一页，每页 20 条
   * 按 d
   * */
  def searchByCondition(): Unit ={

    val searchRequest = new SearchRequest("movie_index")

    // 设置基础检索条件
    val searchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = QueryBuilders.boolQuery()
    val rangeQueryBuilder = QueryBuilders.rangeQuery("doubanScore")
    boolQueryBuilder.filter(rangeQueryBuilder)

    val matchQueryBuilder = QueryBuilders.matchQuery("name", "red sea")
    boolQueryBuilder.must(matchQueryBuilder)
    searchSourceBuilder.query(boolQueryBuilder)

    searchRequest.source(searchSourceBuilder)

    // 设置高亮
    val highlightBuilder = new HighlightBuilder()
    highlightBuilder.field("name")
    searchSourceBuilder.highlighter(highlightBuilder)

    // 设置分页查询
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(2)

    // 设置排序
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)

    // 提交API操作，获取查询结果
    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val totalHits: TotalHits = searchResponse.getHits.getTotalHits
    val value = totalHits.value
    println(s"总共命中${value}条记录")
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (searchHit <- hits) {
      val id = searchHit.getId
      val source = searchHit.getSourceAsString
      println(s"id${id} , source = ${source}")
    }
  }

  /**
   * 查询每位演员参演的电影的平均分，倒叙排序
   */

  def searchAgg(): Unit = {
    val searchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder = new SearchSourceBuilder()
    // 定义聚合操作
    val aggregationBuilder = AggregationBuilders.terms("groupByActorName").field("actorList.name.keyword")
    // 定义子聚合操作
    val subAggregationBuilder = AggregationBuilders.avg("scoreAvg").field("doubanScore")
    // 关联父子聚合操作
    aggregationBuilder.subAggregation(subAggregationBuilder)
    // 子聚合结果排序
    aggregationBuilder.order(BucketOrder.aggregation("scoreAvg",false))
    searchSourceBuilder.aggregation(aggregationBuilder)
    searchRequest.source(searchSourceBuilder)
    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val aggregations: Aggregations = searchResponse.getAggregations
    val groupByActorName: ParsedStringTerms = aggregations.get[ParsedStringTerms]("groupByActorName")
    val buckets: util.List[_ <: Terms.Bucket] = groupByActorName.getBuckets
    import scala.collection.JavaConverters._
    for(bucket <- buckets.asScala){
      val docCount = bucket.getDocCount
      val key = bucket.getKeyAsString
      val avgAggregations: Aggregations = bucket.getAggregations
      val scoreAvg = avgAggregations.get[ParsedAvg]("scoreAvg")
      val scoreAvgValue = scoreAvg.getValueAsString
      println(s"docCount=${docCount}, key=${key}, scoreAvgValue=${scoreAvgValue}")
    }
  }

}


case class Movie(id: Long,name: String)