package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

    @Select("select count(*) from tb_voucher_order where user_id=#{userId} and voucher_id = #{voucherId} ")
    Long selectWithOnlyOne(@Param("userId") Long userId,@Param("voucherId") Long voucherId);
}
