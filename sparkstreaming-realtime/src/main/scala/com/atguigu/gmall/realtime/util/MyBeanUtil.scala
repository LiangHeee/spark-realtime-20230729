package com.atguigu.gmall.realtime.util

import java.lang.reflect.{Field, Modifier}

import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}

import scala.util.control.Breaks

/**
 * @author Hliang
 * @create 2023-08-01 16:34
 */
case object MyBeanUtil {
  def main(args: Array[String]): Unit = {
    val pageLog: PageLog =
      PageLog("mid1001" , "uid101" , "prov101" , null ,null ,null ,null ,null ,null ,null ,null ,null ,null ,0L ,null ,123456)

    val dauInfo: DauInfo = new DauInfo()
    println("拷贝前: " + dauInfo)

    copyProperties(pageLog,dauInfo)

    println("拷贝后: " + dauInfo)
  }

  def copyProperties(src: AnyRef,dest: AnyRef): Unit ={
    // 如果源对象或者目标对象为空，则直接返回
    if(src == null || dest == null) return

    // 我们遍历src的属性，看能不能写入dest同属性
    val srcFields = src.getClass.getDeclaredFields
    for(srcField <- srcFields){
      Breaks.breakable({
          // 判断dest中有没有该属性
          val destField: Field =
        try{
          dest.getClass.getDeclaredField(srcField.getName)
        }catch {
          case ex: NoSuchFieldException => Breaks.break()
        }

        // 判断dest该属性如果是不可变的，那么我们也不处理
        if(destField.getModifiers == Modifier.FINAL){
          Breaks.break()
        }

        // 我们要注意在scala中默认实现了类的getter和setter
        // 其中getter方法为 fieldName()
        // setter方法为 fieldName_$eq()
        val getMethodName = srcField.getName
        val setMethodName = srcField.getName + "_$eq"

        val getMethod =
        try{
           src.getClass.getDeclaredMethod(getMethodName)
        }catch {
          case ex: NoSuchMethodException => Breaks.break()
        }

        val setMethod = dest.getClass.getDeclaredMethod(setMethodName,srcField.getType)
        getMethod.setAccessible(true)
        setMethod.setAccessible(true)

        val srcValue = getMethod.invoke(src)
        setMethod.invoke(dest,srcValue)
      })
    }

  }
}
