package com.hmdp.service.impl;

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
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

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

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 通过id查询秒杀优惠券
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        // 核对时间是否在秒杀范围内
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        LocalDateTime endTime = seckillVoucher.getEndTime();
        // 获取现在时间
        LocalDateTime now = LocalDateTime.now();
        if (!(now.isAfter(beginTime) && now.isBefore(endTime))) {
            return Result.fail("不在活动开始时间内");
        }
        Long userId = UserHolder.getUser().getId();
        // 相同userId的竞争🔒，避免同一用户同时下多个单
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean lock = simpleRedisLock.tryLock(10);
        // 获取代理对象（事务）
        if(!lock) {
            return Result.fail("您已经下过单了");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createOrder(voucherId, seckillVoucher);
        }finally {
            //验证锁是否和存入的一样
            simpleRedisLock.unlock();
        }

    }

    @Transactional
    public Result createOrder(Long voucherId, SeckillVoucher seckillVoucher) {
        // 一个人只能创建一个订单
        Long userId = UserHolder.getUser().getId();
        Long count = voucherOrderMapper.selectWithOnlyOne(userId, voucherId);
        if (count > 0) {
            return Result.fail("用户已购买该优惠券！");
        }
        // 判断库存是否充足
        int stock = seckillVoucher.getStock();
        if (stock <= 0) {
            return Result.fail("库存不足");
        }
        // 扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock-1").eq("voucher_id", voucherId).gt("stock", 0).update();
        if (!success) {
            return Result.fail("库存不足");
        }
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setUserId(userId);
        // 返回订单id
        save(voucherOrder);
        return Result.ok(voucherOrder.getId());
    }
}
