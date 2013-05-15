package com.github.zhangner.sequence.datasource;

/**
 * 用于生成序列号的数据源操作接口
 *
 * @author <a href="mailto:zhangner@gmail.com">ZhanGNer</a>
 * @since 2013-05-14 15:26
 */
public interface Sequence {

    /**
     * 返回下一个序列号
     *
     * @return
     */
    public long nextValue();
}
