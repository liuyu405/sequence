package com.github.zhangner.sequence.datasource;

import com.github.zhangner.sequence.SequenceRange;

import javax.sql.DataSource;
import java.sql.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author <a href="mailto:zhangner@gmail.com">ZhanGNer</a>
 * @since 2013-05-14 15:46
 */
public class MySQLSequence implements Sequence {

    private final Lock lock = new ReentrantLock();

    private static final int RETRY_TIMES = 10;

    /** 当前分配的序列号段 */
    private volatile SequenceRange sequenceRange;

    /** 该序列号表所处的数据源 */
    private DataSource dataSource;

    /** 该序列号分配的最小值 */
    private long min;

    /** 该序列号分配的最大值 */
    private long max;

    /** 该序列号实现每次从MySQL中取得的段落大小 */
    private long step;

    /** 序列号名称 */
    private String sequenceName;

    public MySQLSequence(long min, long max, long step, String sequenceName, DataSource dataSource) {
        this.min = min;
        this.max = max;
        this.step = step;
        this.sequenceName = sequenceName;
        this.dataSource = dataSource;
    }

    @Override
    public long nextValue() {
        if (null == sequenceRange) {
            resetSequenceRange();
        }

        long sequence = sequenceRange.getAndIncrement();
        if (-1 == sequence) {
            // 当前序列号段已经溢出，需要重新生成
            resetSequenceRange();
            return nextValue();
        }

        return sequence;
    }

    private void resetSequenceRange() {
        lock.lock();
        try {
            for (int times = 0; times < RETRY_TIMES; times++) {
                sequenceRange = nextSequenceRange();
                if (sequenceRange != null) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private SequenceRange nextSequenceRange() {
        SequenceRange sequenceRange = null;
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(getQuerySQL());
            statement.setString(1, sequenceName);
            resultSet = statement.executeQuery();

            long oldBoundary = 0;
            // 查询已使用到的序列号异常或者序列号即将达到最大值，剩下的部分不足以支持一个SequenceRange
            if (resultSet.next() || (resultSet.getLong(1) < 0) || (max - oldBoundary < step)) {
                oldBoundary = resultSet.getLong(1);
                sequenceRange = new SequenceRange(oldBoundary + 1, oldBoundary + step);
            } else {
                return null;
            }

            statement = connection.prepareStatement(getUpdateSQL());
            statement.setLong(1, oldBoundary + step);
            statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            statement.setString(3, sequenceName);
            statement.setLong(4, oldBoundary);
            if (statement.executeUpdate() <= 0) {
                return null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(resultSet);
            closeStatement(statement);
            closeConnection(connection);
        }

        return sequenceRange;
    }

    private String getQuerySQL() {
        return "select value from sequences where name = ?";
    }

    private String getUpdateSQL() {
        return "update sequences set value = ?, updated_at = ? where name = ? and value = ?";
    }

    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultSet = null;
        }
    }

    private static void closeStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            statement = null;
        }
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            connection = null;
        }
    }
}
