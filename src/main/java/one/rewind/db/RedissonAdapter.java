package one.rewind.db;

import one.rewind.util.Configs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis Adapter
 * Created by Luke on 1/25/16. 
 * mailto:stormluke1130@gmail.com
 */
public class RedissonAdapter {

	public final static Logger logger = LogManager.getLogger(RedissonAdapter.class.getName());

	private static Map<String, RedissonClient> redisson = new HashMap<>();

	public static RedissonClient get(String name) {

		if(redisson.get(name) != null) return redisson.get(name);

		synchronized (redisson) {

			try {

				com.typesafe.config.Config cfg = Configs.getConfig(RedissonAdapter.class);

				Config config = new Config();

				logger.info("Connecting Redis[{}]...", name);

				config.useSingleServer()
						.setAddress(cfg.getConfig(name).getString("url"))
						.setPassword(cfg.getConfig(name).getString("password"))
						.setConnectionPoolSize(cfg.getConfig(name).getInt("connectionPoolSize"))
						.setSubscriptionConnectionPoolSize(cfg.getConfig(name).getInt("subscriptionConnectionPoolSize"))
						.setTimeout(30000)
						.setRetryAttempts(3)
						.setRetryInterval(1000);

				redisson.put(name, Redisson.create(config));
				logger.info("Connected to Redis[{}]", name);

			} catch (Throwable err) {
				logger.error("Redis init error", err);
				throw err;
			}
		}

		return redisson.get(name);
	}
}
