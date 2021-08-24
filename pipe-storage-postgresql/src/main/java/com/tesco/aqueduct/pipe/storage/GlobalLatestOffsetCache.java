package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.cache.annotation.Cacheable;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
public class GlobalLatestOffsetCache {

    private static final String GET_GLOBAL_LATEST_OFFSET_QUERY = "SELECT value FROM offsets WHERE name='global_latest_offset'";

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PostgresqlStorage.class));

    @Cacheable(value="latest-offset-cache", parameters = "")
    public long get(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();

        try (PreparedStatement statement = connection.prepareStatement(GET_GLOBAL_LATEST_OFFSET_QUERY)) {
             ResultSet resultSet = statement.executeQuery();

             if (resultSet.next()) {
                 return resultSet.getLong(1);
             }

             return 0;
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getLatestOffsetWithConnection:time", Long.toString(end - start));
        }
    }
}
