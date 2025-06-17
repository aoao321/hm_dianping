package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.AliSmsUtils;
import com.hmdp.utils.PasswordEncoder;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private ShopServiceImpl shopService;
    @Autowired
    private ShopMapper shopMapper;
    @Autowired
    private RedisIdWorker redisIdWorker;
    private ExecutorService es = Executors.newFixedThreadPool(500);
    @Autowired
    private AliSmsUtils aliSmsUtils;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ShopTypeMapper shopTypeMapper;


    @Test
    void testPhone(){
        //shopService.saveShopToRedis(1l,10l);
//        try {
//            System.out.println(aliSmsUtils.createClient());
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
        System.out.println(aliSmsUtils.getAccessKeyId());
        System.out.println(aliSmsUtils.getAccessKeySecret());
        try {
            aliSmsUtils.sendVerificationCode("18565402334","1234");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testTime(){
        System.out.println(shopService.isExpired(1l));
    }

    @Test
    void testRedisId() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println(id);
            }
            countDownLatch.countDown();
        };
        long start = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    void test() {
        System.out.println(PasswordEncoder.encode("123456"));
    }

    @Test
    void testShop() throws InterruptedException {
        shopService.saveShopToRedis(1l,10l);
    }

    @Test
    void testQuery(){
        Result result = shopService.queryById(1l);
        System.out.println(result);
    }

    @Test
    void testLoadShopData(){
        // 根据类型添加商品地理位置
        List<ShopType> typeList = shopTypeMapper.selectList(null);
        if (typeList.isEmpty()) {
            System.out.println("没有找到店铺类型数据，跳过加载");
            return;
        }
        for (ShopType type : typeList) {
            Long typeId = type.getId();
            String key = SHOP_GEO_KEY + typeId;
            // 查询分类下所有店铺
            List<Shop> shopList = shopMapper.selectList(new QueryWrapper<Shop>().eq("type_id", typeId));
            if (shopList.isEmpty()) {
                System.out.println("类型ID没有店铺数据");
                continue;
            }
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
            // 加入redis中
            for (Shop shop : shopList) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),new Point(shop.getX(),shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

}
