package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.io.ClassPathResource;

import org.springframework.data.redis.connection.stream.*;

import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service

public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private VoucherOrderMapper voucherOrderMapper;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private ObjectProvider<IVoucherOrderService> proxyProvider;

    //private BlockingQueue<VoucherOrder> orderQueue = new ArrayBlockingQueue<VoucherOrder>(1024*1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(() -> {
            this.proxy = proxyProvider.getObject();
            String queueName = "stream.orders";
            while (true) {
                try {
                    // 消息获取队列中的订单信息
                    //VoucherOrder order = orderQueue.take();

                    // redis获取消息队列
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 如果失败了，说明没有消息，进入下一次循环
                    if (list==null||list.size()==0) {
                        continue;
                    }
                    // 解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder order = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    // 成功，创建订单
                    this.proxy.createOrder(order);
                    // ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {// 抛出异常，则需要检查pendinglist
                    while(true){
                        try {
                            List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                                    Consumer.from("g1", "c1"),
                                    StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                                    StreamOffset.create(queueName, ReadOffset.from("0"))
                            );
                            if (list == null || list.size() == 0) {
                                break;
                            }
                            // 解析消息
                            MapRecord<String, Object, Object> record = list.get(0);
                            Map<Object, Object> map = record.getValue();
                            VoucherOrder order = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                            // 成功，创建订单
                            this.proxy.createOrder(order);
                            // ACK确认 SACK stream.orders g1 id
                            stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                        } catch (Exception ex) {
                            Thread.sleep(200);
                        }
                    }
                }
            }
        });
    }

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                orderId.toString()
                );
        // 判断结果
        int i = result.intValue();
        // 不为0，代表没有资格购买
        if (result != 0) {
            return Result.fail(i ==1 ? "库存不足":"已经下过单");
        }

        //TODO 为0，保存下单信息到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setId(redisIdWorker.nextId("order"));
//        voucherOrder.setUserId(userId);
        // 创建阻塞队列
        //orderQueue.add(voucherOrder);

        //返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 通过id查询秒杀优惠券
//        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
//        // 核对时间是否在秒杀范围内
//        LocalDateTime beginTime = seckillVoucher.getBeginTime();
//        LocalDateTime endTime = seckillVoucher.getEndTime();
//        // 获取现在时间
//        LocalDateTime now = LocalDateTime.now();
//        if (!(now.isAfter(beginTime) && now.isBefore(endTime))) {
//            return Result.fail("不在活动开始时间内");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        // 相同userId的竞争🔒，避免同一用户同时下多个单
//
//        //SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        //boolean lock = simpleRedisLock.tryLock(10);
//
//        // 利用redisson来获取🔒
//        RLock rLock = redissonClient.getLock("order:" + userId);
//        boolean lock = rLock.tryLock();
//        // 获取代理对象（事务）
//        if(!lock) {
//            return Result.fail("您已经下过单了");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createOrder(voucherId, seckillVoucher);
//        }finally {
//            //验证锁是否和存入的一样
//            rLock.unlock();
//        }
//
//    }

    @Transactional
    public void createOrder(VoucherOrder order) {
        Long voucherId = order.getVoucherId();
        // 扣减库存
        seckillVoucherService.update().setSql("stock = stock-1").eq("voucher_id", voucherId).gt("stock", 0).update();
        // 保存
        save(order);
    }
}

