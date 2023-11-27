package com.atguigu.gmall.publisherrealtime.mapper.impl;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;
import com.atguigu.gmall.publisherrealtime.mapper.VisitRealtimeMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-07 22:26
 */
@Slf4j
@Repository
public class VisitRealtimeMapperImpl implements VisitRealtimeMapper {

    @Autowired
    private RestHighLevelClient esClient;

    private String dauIndexPrefix = "gmall_dau_info_";
    private String orderIndexPrefix = "gmall_order_wide_";

    @Override
    public Map<String, Object> searchDauRealtime(String td) {
        HashMap<String, Object> result = new HashMap<>();
        long dauTotal = searchDauTotal(td);
        Map<String, Object> dauTd = searchDauHr(td);
        LocalDate localDate = LocalDate.parse(td);
        LocalDate yt = localDate.minusDays(1);
        Map<String, Object> dauYd = searchDauHr(yt.toString());

        result.put("dauTotal",dauTotal);
        result.put("dauTd",dauTd);
        result.put("dauYd",dauYd);

        return result;
    }

    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<NameValue> result = new ArrayList<>();

        SearchRequest searchRequest = new SearchRequest(orderIndexPrefix + date);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByItemName").field(field).size(100);
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("totalAmountAggs").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms aggregation = aggregations.get("groupByItemName");
            List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String age = bucket.getKeyAsString();
                Aggregations subAggs = bucket.getAggregations();
                ParsedSum totalAmountAggs = subAggs.get("totalAmountAggs");
                double totalAmount = totalAmountAggs.getValue();
                result.add(new NameValue(age,totalAmount));
            }
        } catch (ElasticsearchStatusException e){
            if(e.status() == RestStatus.NOT_FOUND){
                log.error("es中找不到该索引！");
            }
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Map<String, Object> searchDetailByItem(String itemName, String date, Long pageNum, Long pageSize) {
        HashMap<String, Object> result = new HashMap<>();
        ArrayList<Map<String, Object>> list = new ArrayList<>();

        SearchRequest searchRequest = new SearchRequest(orderIndexPrefix + date);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"create_time","order_price","province_name",
                "sku_name","sku_num","total_amount","user_age","user_gender"},null);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.from(pageNum.intValue());
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("sku_name");
        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.size(20);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            // 获取总数
            long total = searchResponse.getHits().getTotalHits().value;
            result.put("total",total);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            for (SearchHit searchHit : searchHits) {
                // 获取数据
                Map<String, Object> orderWideMap = searchHit.getSourceAsMap();
                // 获取高亮
                HighlightField highlightField = searchHit.getHighlightFields().get("sku_name");
                Text[] fragments = highlightField.getFragments();
                if(fragments != null && fragments.length > 0){
                    orderWideMap.put("sku_name",fragments[0].toString());
                }
                list.add(orderWideMap);
            }
            result.put("detail",list);
        } catch (ElasticsearchStatusException e){
            if(e.status() == RestStatus.NOT_FOUND){
                log.error("es中找不到该索引！");
            }
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private long searchDauTotal(String td){
        SearchRequest searchRequest = new SearchRequest(dauIndexPrefix + td);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        long dauTotal = 0;
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            TotalHits totalHits = searchResponse.getHits().getTotalHits();
            dauTotal = totalHits.value;
        } catch (ElasticsearchStatusException e) {
            if(e.status() == RestStatus.NOT_FOUND){
                log.error("es中找不到该索引！");
            }
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauTotal;
    }

    private Map<String,Object> searchDauHr(String td){
        HashMap<String, Object> map = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest(dauIndexPrefix + td);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByHr").field("hr");
        termsAggregationBuilder.size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupByHr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long docCount = bucket.getDocCount();
                map.put(hr,docCount);
            }

        } catch (ElasticsearchStatusException e) {
            if(e.status() == RestStatus.NOT_FOUND){
                log.error("es中找不到该索引！");
            }
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }



}
