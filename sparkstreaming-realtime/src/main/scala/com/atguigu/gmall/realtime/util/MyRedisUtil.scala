package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Reids工具类，从JedisPool中获取Jedis连接
 *
 * @author Hliang
 * @create 2023-07-30 15:30
 */
object MyRedisUtil {

  var jedisPool: JedisPool = null

  def getJedisClientFromPool(): Jedis ={
    if(jedisPool == null){
      val host : String = MyPropertiesUtil(MyConfig.REDIS_HOST)
      val port : String = MyPropertiesUtil(MyConfig.REDIS_PORT)
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }

    jedisPool.getResource
  }

}
