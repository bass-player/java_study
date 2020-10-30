package red.reid.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Redisson配置连接类
 */
public class RedissonAdapter {

	public final static Logger logger = LogManager.getLogger(RedissonAdapter.class.getName());

	public static RedissonClient redisson;

	/**
	 * 连接池初始化方法
	 */
	static {

		String url = "redis://127.0.0.1:6379";
		String passwd = "dw@@2018";

		Config config = new Config();

		config.useSingleServer()
				.setAddress(url)
				.setPassword(passwd)
				.setConnectionPoolSize(50)
				.setSubscriptionConnectionPoolSize(50)
				.setTimeout(30000)
				.setRetryAttempts(3)
				.setRetryInterval(1000);

		redisson = Redisson.create(config);

		logger.info("Connected to Redis[{}]", url);
	}
}
