package com.atguigu.gmall.publisherrealtime.controller;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;
import com.atguigu.gmall.publisherrealtime.service.VisitRealtimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-07 22:24
 */
@RestController
public class PublisherController {

    @Autowired
    private VisitRealtimeService visitRealtimeService;

    @GetMapping("dauRealtime")
    public Map<String,Object> dauRealtime(@RequestParam("td")String td){
        Map<String,Object> result = visitRealtimeService.doDauRealtime(td);
        return result;
    }

    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(@RequestParam("itemName")String itemName,
                                       @RequestParam("date")String date,
                                       @RequestParam("t")String t){
        List<NameValue> result = visitRealtimeService.doStatsByItem(itemName,date,t);
        return result;
    }

    @GetMapping("detailByItem")
    public Map<String,Object> detailByItem(@RequestParam("itemName")String itemName,
                                           @RequestParam("date")String date,
                                           @RequestParam(value = "pageNo",required = false,defaultValue = "1")Long pageNo,
                                           @RequestParam(value = "pageSize",required = false,defaultValue = "20")Long pageSize){
        Map<String,Object> result = visitRealtimeService.doDetailByItem(itemName,date,pageNo,pageSize);
        return result;
    }

}
