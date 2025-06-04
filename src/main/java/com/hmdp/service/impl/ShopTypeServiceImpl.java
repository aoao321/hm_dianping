package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private ShopTypeMapper shopTypeMapper;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        String typeKey = RedisConstants.CACHE_TYPE_KEY;
        //从redis中查询类型数据
        String typeJSON = stringRedisTemplate.opsForValue().get(typeKey);
        //存在，直接返回数据
        if (StringUtils.isNotBlank(typeJSON)) {
            //解析成java对象
            List<ShopType> types = JSONUtil.toList(typeJSON, ShopType.class);
            return Result.ok(types);
        }
        //不存在，从数据库中查询
        List<ShopType> typeList = shopTypeMapper.selectList(null);
        if (typeList != null && typeList.size() > 0) {
            stringRedisTemplate.opsForValue().set(typeKey, JSONUtil.toJsonStr(typeList));
            stringRedisTemplate.expire(typeKey,RedisConstants.CACHE_TYPE_TTL,TimeUnit.MINUTES);
            return Result.ok(typeList);
        }else {
            return Result.fail("类型不存在");
        }
    }
}
