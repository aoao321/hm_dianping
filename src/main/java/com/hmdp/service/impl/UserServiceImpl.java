package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
@Transactional
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private UserMapper userMapper;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private AliSmsUtils aliSmsUtils;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //校验手机号格式
        //不符合，返回错误信息
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }
        //符合，生成随机6位数验证码
        String code = RandomUtil.randomNumbers(6);
        //保存验证保存到session
        //session.setAttribute("code", code);

        //保存到redis中
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY +phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //发送验证码到用户手机号上
        aliSmsUtils.sendVerificationCode(phone,code);
        log.info("code:",code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
        return Result.fail("手机号格式错误");
        }
            //验证码登录
            if (StringUtils.isNotBlank(loginForm.getCode())) {
                return loginByCode(loginForm, session);
            }
            //手机登录
            else if (StringUtils.isNotBlank(loginForm.getPassword())) {
                return loginByPassword(loginForm, session);
            }
        return Result.ok();
    }

    @Override
    public Result sign() {
        // 获取用户
        Long id = UserHolder.getUser().getId();
        // 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyMMdd"));
        String key = RedisConstants.USER_SIGN_KEY + id + keySuffix;
        // 获取天数
        int dayOfMonth = now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 获取用户
        Long id = UserHolder.getUser().getId();
        // 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyMMdd"));
        String key = RedisConstants.USER_SIGN_KEY + id + keySuffix;
        // 获取天数
        int dayOfMonth = now.getDayOfMonth();
        // 获取本月截止今天所有签到记录
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (result == null || result.isEmpty()){
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num == null || num ==0){
            return Result.ok(0);
        }
        // 循环遍历
        int count = 0;
        while(true){
            // 与1
            if ((num & 1)==0){
                break;
            }else {
                count++;
            }
            // 移动一位
            count >>>=1;
        }
        return Result.ok(count);
    }

    private Result loginByCode(LoginFormDTO loginForm, HttpSession session) {
        // 校验验证码
//        String cacheCode = (String) session.getAttribute("code");
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + loginForm.getPhone());
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("验证码错误");
        }

        //根据手机号查询数据库中是否存在该用户
        QueryWrapper<User> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("phone", loginForm.getPhone());
        User user = userMapper.selectOne(queryWrapper);

        // 不存在则创建新用户
        if (user == null) {
            user = createUserWithPhone(loginForm.getPhone());
        }

        // 登录成功，存储用户信息到redis
        //生成token
        String token = UUID.randomUUID().toString(true);
        //转换为hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 强制转换数值类型为字符串
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new LinkedHashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) ->
                                fieldValue != null ? fieldValue.toString() : null
                        )
        );
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,userMap);
        //设置有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.LOGIN_USER_TTL,TimeUnit.MINUTES);
        //session.setAttribute("user", user);
        //返回token
        return Result.ok(token);
    }

    private Result loginByPassword(LoginFormDTO loginForm, HttpSession session) {
        if (loginForm.getPassword() == null) {
            return Result.fail("请输入密码");
        }
        //根据手机号查询数据库中是否存在该用户
        QueryWrapper<User> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("phone", loginForm.getPhone());
        User user = userMapper.selectOne(queryWrapper);
        //获取到密码和数据库中加密后的密码核对
        if (user == null) {
            return Result.fail("该手机号未注册");
        }
        //数据库密码
        String userPassword = user.getPassword();
        //用户输入密码
        String loginFormPassword = loginForm.getPassword();
        String encode = PasswordEncoder.encode(loginFormPassword);
        if (!userPassword.equals(encode)) {
            return Result.fail("密码错误");
        }
        //保存到redis中
        //session.setAttribute("user", user);

        // 登录成功，存储用户信息到redis
        //生成token
        String token = UUID.randomUUID().toString(true);
        //转换为hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 强制转换数值类型为字符串
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new LinkedHashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) ->
                                fieldValue != null ? fieldValue.toString() : null
                        )
        );
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,userMap);
        //设置有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.LOGIN_USER_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = User.builder()
                .phone(phone)
                .updateTime(LocalDateTime.now())
                .createTime(LocalDateTime.now())
                .nickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10))
                .password(PasswordEncoder.encode(SystemConstants.INIT_PASSWORD))
                .build();
        userMapper.insert(user);
        return user;
    }
}
