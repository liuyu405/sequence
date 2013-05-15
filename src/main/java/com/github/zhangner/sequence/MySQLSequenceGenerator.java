package com.github.zhangner.sequence;

import com.github.zhangner.sequence.datasource.MySQLSequence;

import javax.sql.DataSource;
import java.util.*;

/**
 * 使用MySQL作为数据源的序列号生成器实现<br>
 * 实现原理参考Oracle，第一次分配序列号时向MySQL申请一段序列号(大小由step设定)，从第一个开始分配给应用，
 * 分配完该段序列号之后，再向MySQL申请下一个序列号段进行分配。
 * 通过设置最小值、最大值的方式，来保证生成的序列号处理该区间之中。
 *
 * <p>支持配置多个MySQL数据源，对于不指定数据源的序列号生成，随机选择数据源进行生成。</p>
 *
 * <pre>
 * 对于支持序列号的MySQL数据源，需要提供以下表结构的表名为sequences的数据表：
 *  字段名          类型
 *  name            varchar(32)
 *  value           bigint
 *  updated_at      datetime
 * </pre>
 * 该表中一行记录代表一个序列号，name字段为该序列号名称，value为该序列号已分配到的值，updated_at为最后分配时间。
 *
 * @author <a href="mailto:zhangner@gmail.com">ZhanGNer</a>
 * @since 2013-05-14 15:24
 */
public class MySQLSequenceGenerator implements SequenceGenerator {

    /** 用于生成序列号的MySQL数据源配置 */
    private Map<String, DataSource> dataSourceMap;

    private Map<String, MySQLSequence> mySQLSequences;

    /** 最小的序列号 */
    private long min = 1;

    /** 最大的序列号，达到该值后会重新从最小的序列号开始生成 */
    private long max = Long.MAX_VALUE;

    /** 每次从MySQL中取出的一段序列号大小，该值设置太小会频繁操作DB，设置太大会在应用重启等操作时浪费序列号 */
    private long step = 1000;

    /** 序列号名称，用于和其他序列号区别 */
    private String sequenceName = "default_sequence";

    public MySQLSequenceGenerator(Map<String, DataSource> dataSourceMap, long min, long max, long step, String sequenceName) {
        this.dataSourceMap = dataSourceMap;
        this.min = min;
        this.max = max;
        this.step = step;
        this.sequenceName = sequenceName;
        afterProperties();
    }

    @Override
    public void afterProperties() {
        if ((null == dataSourceMap) || (dataSourceMap.size() == 0)) {
            throw new RuntimeException("There's no configed DataSource");
        }

        for (Map.Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
            mySQLSequences.put(entry.getKey(),
                               new MySQLSequence(min, max, step, sequenceName, entry.getValue()));
        }
    }


    @Override
    public long generate() {
        if (mySQLSequences.size() == 1) {
            return mySQLSequences.get(0).nextValue();

        } else {
            return generateFromAvailable(new ArrayList<MySQLSequence>(mySQLSequences.values()));
        }
    }

    @Override
    public long generate(String dsNum) {
        return mySQLSequences.get(dsNum).nextValue();
    }

    private long generateFromAvailable(List<MySQLSequence> availableSequence) {
        if ((null == availableSequence) || (availableSequence.size() == 0)) {
            throw new RuntimeException("There's no available datasource to generate sequence.");
        }

        int dsNum = new Random().nextInt(availableSequence.size());
        try {
            long sequence = availableSequence.get(dsNum).nextValue();
            return sequence * 10 + dsNum;

        } catch (Throwable throwable) {
            throwable.printStackTrace();
            availableSequence.remove(dsNum);
            return generateFromAvailable(availableSequence);
        }
    }

    public void setDataSourceMap(Map<String, DataSource> dataSourceMap) {
        this.dataSourceMap = dataSourceMap;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public void setStep(long step) {
        this.step = step;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }
}
