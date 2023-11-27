package com.atguigu.gmall.publisherrealtime.service;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-07 22:25
 */
public interface VisitRealtimeService {
    Map<String, Object> doDauRealtime(String td);

    List<NameValue> doStatsByItem(String itemName, String date, String t);

    Map<String, Object> doDetailByItem(String itemName, String date, Long pageNo, Long pageSize);
}
