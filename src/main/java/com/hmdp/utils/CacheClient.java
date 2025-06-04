package com.hmdp.utils;

import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * @author aoao
 * @create 2025-05-30-13:36
 */
@Component
@Slf4j
public class CacheClient {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 存数据入redis并且设置过期时间
     *
     * @param key
     * @param value
     * @param expire
     * @param timeUnit
     */
    public void set(String key, Object value, Long expire, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expire, timeUnit);
    }

    /**
     * 存数据入redis并且设置逻辑过期时间
     *
     * @param key
     * @param value
     * @param expire
     * @param timeUnit
     */
    public void setWithLogicalExpire(String key, Object value, Long expire, TimeUnit timeUnit) {
        //过期时间
        //LocalDateTime.now().plusSeconds(timeUnit.toSeconds(expire));
        //先转换为redisData数据存储逻辑时间
        RedisData<Object> RedisData = new RedisData<>(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(expire)), value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(RedisData));
    }

    /**
     * 解决缓存穿透
     * @param keyPrefix 前缀
     * @param id id
     * @param clazz 返回的实际类型
     * @param expire 过期时间
     * @param timeUnit 时间单位
     * @param function 函数式接口，传入查询数据库的逻辑
     * @return
     * @param <T>
     */
    public <T> T queryWithPassThrough(String keyPrefix, Long id, Class<T> clazz, Long expire, TimeUnit timeUnit, Function<Long, T> function) {
        String key = keyPrefix + id;
        //查询
        String JSON = stringRedisTemplate.opsForValue().get(key);
        //存在直接返回
        if (StringUtils.isNotBlank(JSON)) {
            T t = JSONUtil.toBean(JSON, clazz);
            return t;
        }
        //存在该对象但是为空字符串返回不存在
        if (JSON != null) {
            return null;
        }
        //为null则需要查询数据库，进行缓存重建
        T t = function.apply(id);
        //不存在返回空值到redis
        if (t == null) {
            //设置两分钟过期时间
            stringRedisTemplate.opsForValue().set(key, "",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }else {
            set(key,t,expire,timeUnit);
            return t;
        }
    }


    /**
     * 利用逻辑过期解决缓存击穿
     * @param keyPrefix
     * @param lockPrefix
     * @param id
     * @param clazz
     * @param expire
     * @param timeUnit
     * @return
     * @param <T>
     */
    public <T> T queryWithLogicalExpire(String keyPrefix, String lockPrefix,Long id, Class<T> clazz, Long expire, TimeUnit timeUnit,Function<Long, T> function) {
        //拼接key
        String key = keyPrefix + id;
        String JSON = stringRedisTemplate.opsForValue().get(key);
        //不存在返回null
        if (StringUtils.isBlank(JSON)) {
            return null;
        }
        //命中，把json转换为Java对象
        RedisData redisData = JSONUtil.toBean(JSON,RedisData.class);
        //获取过期时间和属性
        LocalDateTime expireTime = redisData.getExpireTime();
        T data = JSONUtil.toBean((JSONObject) redisData.getData(), clazz);
        //如果过期，进行缓存重建
        if (!expireTime.isBefore(LocalDateTime.now())) {
            return data;
        }
        //获取lockKey
        String lockKey =  lockPrefix + id;
        boolean lock = tryLock(lockKey);
        //获得🔒成功，进行重建
        if (lock) {
            //成功，开启独立线程重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //重建
                try {
                    //查询数据库
                    T t = function.apply(id);
                    //写入redis中
                    setWithLogicalExpire(key,t,expire,timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
            return data;
        }
        return data;
    }



    /**
     * 尝试获取锁，写入数据到redis
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
