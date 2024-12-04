package com.sim.spriced.platform.Utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.concurrent.Callable;

public class RetryUtility {

    private static final Logger logger = LoggerFactory.getLogger(RetryUtility.class);

    /**
     Utility function to retry connections which accept a callable function which will be retried for {@int maxAttempts} with {@long delays} between each attempts
     */
    public static <T> void retryConnections(Callable<T> callable, int maxAttempts, long delay, String connectionName) throws Exception {
        retry(callable, maxAttempts, delay, connectionName, 1, null);
    }

    private static <T> void retry(Callable<T> callable, int maxAttempts, long delay, String connectionName, int attempt, Exception lastException) throws Exception {
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();

        if (attempt > maxAttempts) {
            logger.error("{}.{} - {}: All {} attempts failed. Throwing last exception.", className, methodName, connectionName, maxAttempts);
            Assert.notNull(lastException, "Exception not recorded");
            throw lastException;
        }

        try {
            logger.info("{}.{} - {}: Attempt {}", className, methodName, connectionName, attempt);
            callable.call();
            logger.info("{}.{} - {}: Connection successful on attempt {}", className, methodName, connectionName, attempt);
        } catch (Exception e) {
            logger.error("{}.{} - {}: Failed attempt {}, caused by: {}", className, methodName, connectionName, attempt, e.getMessage());
            Thread.sleep(attempt * delay);
            retry(callable, maxAttempts, delay, connectionName, attempt + 1, e);
        }
    }


}
