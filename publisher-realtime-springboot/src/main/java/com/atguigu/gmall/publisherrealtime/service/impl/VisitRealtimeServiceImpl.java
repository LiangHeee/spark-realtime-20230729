package com.atguigu.gmall.publisherrealtime.service.impl;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;
import com.atguigu.gmall.publisherrealtime.mapper.VisitRealtimeMapper;
import com.atguigu.gmall.publisherrealtime.service.VisitRealtimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-08-07 22:26
 */
@Service
public class VisitRealtimeServiceImpl implements VisitRealtimeService {

    @Autowired
    private VisitRealtimeMapper visitRealtimeMapper;

    @Override
    public Map<String, Object> doDauRealtime(String td) {
        Map<String,Object> result = visitRealtimeMapper.searchDauRealtime(td);
        return result;
    }

    @Override
    public List<NameValue> doStatsByItem(String itemName, String date, String t) {
        ArrayList<NameValue> result = new ArrayList<>();
        String field = tConvertToField(t);
        List<NameValue> searchResult = visitRealtimeMapper.searchStatsByItem(itemName,date,field);
        if(searchResult != null && searchResult.size() > 0){

            double totalAmount20 = 0;
            double totalAmount20To29 = 0;
            double totalAmount30 = 0;
            double maleTotalAmount = 0;
            double feMaleTotalAmount = 0;
            for (NameValue nameValue : searchResult) {
                if("gender".equals(t)){
                    if("F".equals(nameValue.getName())){
                        feMaleTotalAmount += (Double) nameValue.getValue();
                    }else {
                        maleTotalAmount += (Double)nameValue.getValue();
                    }
                }

                if("age".equals(t)){
                    long age = Long.parseLong(nameValue.getName());
                    if(age < 20){
                        totalAmount20 += (Double)nameValue.getValue();
                    }else if(age <= 20 && age < 30){
                        totalAmount20To29 += (Double)nameValue.getValue();
                    }else {
                        totalAmount30 += (Double)nameValue.getValue();
                    }
                }
            }

            result.clear();
            if("gender".equals(t)){
                result.add(new NameValue("男",maleTotalAmount));
                result.add(new NameValue("女",feMaleTotalAmount));
            }

            if("age".equals(t)){
                result.add(new NameValue("20岁以下",totalAmount20));
                result.add(new NameValue("20岁至29岁",totalAmount20To29));
                result.add(new NameValue("30岁以上",totalAmount30));
            }
        }
        return result;
    }

    @Override
    public Map<String, Object> doDetailByItem(String itemName, String date, Long pageNo, Long pageSize) {
        Long pageNum = (pageNo - 1) * pageSize;
        Map<String,Object> result = visitRealtimeMapper.searchDetailByItem(itemName,date,pageNum,pageSize);
        return result;
    }

    private String tConvertToField(String t){
        if("age".equals(t)){
            return "user_age";
        }else if("gender".equals(t)){
            return "user_gender";
        }else{
            return null;
        }
    }
}
