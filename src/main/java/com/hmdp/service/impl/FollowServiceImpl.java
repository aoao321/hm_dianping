package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private UserMapper userMapper;

    public FollowServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long currentUserId = UserHolder.getUser().getId();
        String key = RedisConstants.FELLOW_KEY+currentUserId;
        // 根据isFollow判断是关注还是取关
        if (isFollow){
            // 关注，新增用户
            Follow follow = new Follow();
            follow.setUserId(currentUserId);
            follow.setFollowUserId(id);
            boolean save = save(follow);
            if (save){
                // 把关注的用户id，放入redis的set集合
                stringRedisTemplate.opsForSet().add(key,id.toString());
            }
        }else {
            // 取关，删除
            remove(new QueryWrapper<Follow>().eq("user_id", currentUserId).eq("follow_user_id", id));
            // 从redis中移除
            stringRedisTemplate.opsForSet().remove(key,id.toString());
        }
        return Result.ok();
    }

    @Override
    public Result followOrNot(Integer id) {//对方的id
        Long currentUserId = UserHolder.getUser().getId();
        String key = RedisConstants.FELLOW_KEY+currentUserId;
        //查询是否存在
        //Integer count = query().eq("user_id", currentUserId).eq("follow_user_id", id).count();
        Boolean isExist = stringRedisTemplate.opsForSet().isMember(key, id.toString());
        if (isExist){
            return Result.ok(true);
        }else {
            return Result.ok(false);
        }
    }

    @Override
    public Result followCommon(Integer id) {//对方的id
        Long currentUserId = UserHolder.getUser().getId();
        String myKey = RedisConstants.FELLOW_KEY+currentUserId;
        String key = RedisConstants.FELLOW_KEY+id;
        //取set中的交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(myKey, key);
        List<Long> followCommon = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询共同关注
        List<User> users = userMapper.selectBatchIds(followCommon);
        List<UserDTO> dtoList = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(dtoList);
    }
}
