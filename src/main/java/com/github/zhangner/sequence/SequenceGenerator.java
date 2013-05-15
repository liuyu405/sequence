package com.github.zhangner.sequence;

/**
 * Sequence生成器
 * <p></p>
 *
 * @author <a href="mailto:zhangner@gmail.com">ZhanGNer</a>
 * @since 2013-05-14 15:17
 */
public interface SequenceGenerator {

    /**
     * 基于设置的属性进行初始化
     */
    public void afterProperties();

    /**
     * 生成序列号，如果配置有多个数据源，则随机从其中选择一个进行生成。
     * 如果某个数据源生成序列号时异常，则从其他数据源继续生成，直到所有数据源都已经尝试或者生成成功。
     *
     * @return
     */
    public long generate();

    /**
     * 从指定数据源生成序列号
     *
     * @param dsNum     指定的数据源编号，从数据源Map中的key值一致
     * @return
     */
    public long generate(String dsNum);

}
