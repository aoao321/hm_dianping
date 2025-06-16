package com.hmdp.controller;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;
import vo.BlogVo;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/blog")
public class BlogController {

    @Resource
    private IBlogService blogService;
    @Resource
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private UserMapper userMapper;

    @PostMapping
    public Result saveBlog(@RequestBody Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        blogService.save(blog);
        // 返回id
        return Result.ok(blog.getId());
    }

    @PutMapping("/like/{id}")
    public Result likeBlog(@PathVariable("id") Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        //一个用户只能点赞一次，再点击则取消点赞
        Long userId = UserHolder.getUser().getId();
        if (UserHolder.getUser()==null){
            return Result.fail("未登录");
        }
        //通过set判断当前，点评是否存在该用户id
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score==null) {//不存在
            boolean isSuccess = blogService.update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess) {
                //加入到sorted set中
                stringRedisTemplate.opsForZSet().add(key, userId.toString(),System.currentTimeMillis());
            }
        } else {//失败
            //说明已经点过赞了
            boolean isSuccess = blogService.update().setSql("liked = liked - 1").eq("id", id).update();
            //redis中移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

    @GetMapping("/of/me")
    public Result queryMyBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", user.getId()).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    @GetMapping("/hot")
    public Result queryHotBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            Long userId = blog.getUserId();
            User user = userService.getById(userId);
            blog.setName(user.getNickName());
            blog.setIcon(user.getIcon());
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @GetMapping("/{id}")
    public Result queryBlog(@PathVariable("id") Long id) {
        //查询信息
        Blog blog = blogService.getById(id);
        if (blog == null) {
            return Result.fail("不存在");
        }
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        //设置属性
        blog.setIcon(user.getIcon());
        blog.setName(user.getNickName());

        isBlogLiked(blog);

        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();

        // 先检查用户是否登录
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，默认未点赞
            blog.setLiked(0);
            return;
        }
        // 用户已登录，检查是否点赞
        Long userId = user.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setLiked(score==null ? 0 : 1);
    }

    @GetMapping("/likes/{id}")
    public Result queryLikes(@PathVariable("id") Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Blog blog = blogService.getById(id);
        if (blog == null) {
            return Result.fail("不存在");
        }
        //通过key查询排名前5的用户
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.size() == 0) {
            return Result.ok();
        }
        //解析出用户id
        List<Long> userIdList = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //根据用户id查询用户
        List<User> users = userMapper.selectBatchIds(userIdList);
        List<UserDTO> dtoList = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        //返回
        return Result.ok(dtoList);
    }

    @GetMapping("/of/user")
    public Result queryMyUser(@RequestParam(value = "current", defaultValue = "1") Integer current, @RequestParam("id") Long id) {
        Page<Blog> page = blogService.query().eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }
}
