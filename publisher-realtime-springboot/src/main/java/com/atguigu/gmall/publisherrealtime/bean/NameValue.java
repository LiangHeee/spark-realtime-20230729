package com.atguigu.gmall.publisherrealtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Hliang
 * @create 2023-08-08 9:43
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class NameValue {
    private String name;
    private Object value;
}
