package com.hmdp;

import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.AliSmsUtils;
import com.hmdp.utils.PasswordEncoder;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
}
