package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

/**
 * @author Hliang
 * @create 2023-07-29 20:53
 */
object MyPropertiesUtil {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key: String): String = {
    bundle.getString(key)
  }

}
