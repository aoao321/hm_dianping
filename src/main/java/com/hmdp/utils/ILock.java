package com.hmdp.utils;

/**
 * @author aoao
 * @create 2025-06-10-15:07
 */
public interface ILock {

    boolean tryLock(long timeout);

    void unlock();
}
