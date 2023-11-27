package com.atguigu.gmall.publisherrealtime.mapper;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-07 22:26
 */
public interface VisitRealtimeMapper {
    Map<String, Object> searchDauRealtime(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String itemName, String date, Long pageNum, Long pageSize);
}
