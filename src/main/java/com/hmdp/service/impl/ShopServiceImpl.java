package com.hmdp.service.impl;

import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisCommands;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.RedisCommand;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ShopMapper shopMapper;
    @Autowired
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透解决方法
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = queryWithMutex(id);
        //Shop shop = queryWithLogicalExpire(id);

        //Shop shopTest = shopMapper.selectById(1l);
        //cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY+1l,shopTest,CACHE_SHOP_TTL,TimeUnit.SECONDS);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, LOCK_SHOP_KEY, id, Shop.class, CACHE_SHOP_TTL, TimeUnit.SECONDS, id2 -> getById(id2));
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private Shop queryWithLogicalExpire(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        //从redis查询是否存在店铺id的数据
        String shopJSON = stringRedisTemplate.opsForValue().get(shopKey);
        //未命中，直接返回null
        if (StringUtils.isBlank(shopJSON)) {
            return null;
        }
        RedisData<Shop> redisData = JSONUtil.toBean(
                shopJSON,
                new TypeReference<RedisData<Shop>>() {},
                true
        );
        Shop shop = redisData.getData();
        //判断是否过期
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期，直接返回
        if (!expireTime.isBefore(LocalDateTime.now())) {
            return shop;
        }
        //已过期，需要缓存重建
        //获取互斥锁
        String lockKey = LOCK_SHOP_KEY+id;
        boolean lock = tryLock(lockKey);
        //判断获取锁🔒是否成功
        if (lock) {
            //成功，开启独立线程重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //重建
                try {
                    saveShopToRedis(id,30l);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }

        //直接返回旧数据
        return shop;
    }

    private Shop queryWithMutex(Long id) {
        String shopKey = CACHE_SHOP_KEY+id;
        //从redis查询是否存在店铺id的数据
        String shopJSON = stringRedisTemplate.opsForValue().get(shopKey);
        //有数据，直接返回
        if (StringUtils.isNotBlank(shopJSON)){
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return shop;
        }
        // 判断命中的是否是空字符串
        if(shopJSON!=null){
            return null;
        }

        //redis中不存在店铺的缓存数据实现缓存重建
        //1.获取互斥🔒
        String lockKey = "lock:shop:"+id;
        try {
            boolean lock = tryLock(lockKey);
            //2.判断获取是否成功
            if (!lock){
                //失败，休眠然后重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 双重检查：获取锁后再次检查缓存，避免多线程重复重建
            shopJSON = stringRedisTemplate.opsForValue().get(shopKey);
            if (shopJSON != null) {
                return shopJSON.isEmpty() ? null : JSONUtil.toBean(shopJSON, Shop.class);
            }

            //成功，查询数据库进行redis写入的操作，最后释放🔒
            Shop shop = shopMapper.selectById(id);
            //模拟重建延迟
            Thread.sleep(200);
            if (shop!=null){
                //存在该店铺，则写入redis并且返回
                stringRedisTemplate.opsForValue().set(shopKey,JSONUtil.toJsonStr(shop));
                //超时剔除
                stringRedisTemplate.expire(shopKey,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
                return shop;
            }//不可以直接返回404，存在穿透的问题
            else {
                //将空值写入redis中
                stringRedisTemplate.opsForValue().set(shopKey,null);
                //设置过期时间
                stringRedisTemplate.expire(shopKey,RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }

    }

    public void saveShopToRedis(Long id,Long expireSeconds) throws InterruptedException {
        // 查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 封装逻辑过期
        RedisData<Shop> redisData = new RedisData<>(LocalDateTime.now().plusSeconds(expireSeconds), shop);
        // 写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    public boolean isExpired(Long id){
        LocalDateTime now = LocalDateTime.now();
        //获取过期时间
        String shopJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        RedisData<Shop> redisData = JSONUtil.toBean(shopJSON, RedisData.class);
        //判断是否超时
        LocalDateTime expireTime = redisData.getExpireTime();
        return expireTime.isBefore(now);
    }

    private Shop queryWithPassThrough(Long id) {
        String shopKey = CACHE_SHOP_KEY+id;
        //从redis查询是否存在店铺id的数据
        String shopJSON = stringRedisTemplate.opsForValue().get(shopKey);
        //有数据，直接返回
        if (StringUtils.isNotBlank(shopJSON)){
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return shop;
        }
        // 判断命中的是否是空字符串
        if(shopJSON!=null){
            return null;
        }
        //没有，查询数据库
        Shop shop = shopMapper.selectById(id);
        if (shop!=null){
            //存在该店铺，则写入redis并且返回
            stringRedisTemplate.opsForValue().set(shopKey,JSONUtil.toJsonStr(shop));
            //超时剔除
            stringRedisTemplate.expire(shopKey,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return shop;
        }//不可以直接返回404，存在穿透的问题
        else {
            //将空值写入redis中
            stringRedisTemplate.opsForValue().set(shopKey,null);
            //设置过期时间
            stringRedisTemplate.expire(shopKey,RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
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


    @Override
    @Transactional
    public void updateShop(Shop shop) {
        Long shopId = shop.getId();
            //更新数据库
            shopMapper.updateById(shop);
            //删除缓存
            stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
    }

    @Override
    public Result queryShopByTypeWithXY(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否要查坐标
        if (x==null||y==null){//为空直接返回数据库中所有该分类下的数据
            // 根据类型分页查询
            Page<Shop> page = shopMapper.selectPage(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE),
                    new QueryWrapper<Shop>().eq("type_id", typeId));
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 按照距离排序
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> search = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if (search == null){
            return Result.ok();
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = search.getContent();
        // 截取from到end
        List<Long> ids = new ArrayList<>();
        Map<String,Distance> distanceMap = new HashMap<>();
        list.stream().skip(from).forEach(result ->{
            String shopIdStr = result.getContent().getName();
            Distance distance = result.getDistance();
            ids.add(Long.parseLong(shopIdStr));
            distanceMap.put(shopIdStr,distance);
        });
        if (ids == null || ids.size() == 0) {
            return Result.ok();
        }
        // 解析出id，查询shop
        String idStr = StrUtil.join(",",ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD (id," + idStr + ")").list();
        if (shops==null){
            return Result.ok();
        }
        // 返回
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
