package red.reid.common.test;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.junit.Test;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RMap;
import red.reid.common.RedissonAdapter;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RedissonAdapterTest {

	// https://github.com/redisson/redisson/wiki/7.-distributed-collections
	RMap<String, String> map = RedissonAdapter.redisson.getMap("Test-Map");

	//
	RBlockingQueue<String> queue = RedissonAdapter.redisson.getBlockingQueue("Test-Queue");

	// https://github.com/redisson/redisson/wiki/6.-distributed-objects
	RAtomicLong atomicLong = RedissonAdapter.redisson.getAtomicLong("Test-Atomic-Long");

	/**
	 * 测试RMap的远程写入
	 */
	@Test
	public void testRMap1() {

		for(int i=0; i<10; i++) {
			map.put(String.valueOf(i), i + "==" + new Random().nextInt(100));
		}
	}

	@Test
	public void testRMap2() {

		for(int i=0; i<10; i++) {
			RedissonAdapter.logger.info("Get {}", map.get(String.valueOf(i)));
		}
	}

	@Test
	public void testRQueue() throws InterruptedException {

		ExponentialDistribution ed = new ExponentialDistribution(1000D/100);

		int count = 10000;

		CountDownLatch countDownLatch = new CountDownLatch(count);

		ExecutorService es = Executors.newFixedThreadPool(4);

		for(int i=0; i<2; i++) {
			es.submit(() -> {

				while (atomicLong.get() < count) {
					queue.add(String.valueOf(atomicLong.incrementAndGet()));
					try {
						Thread.sleep((long) ed.sample());
					} catch (InterruptedException e) {
						RedissonAdapter.logger.error("Error, ", e);
					}
				}
			});
		}

		for(int i=0; i<2; i++) {
			es.submit(() -> {

				String val;
				while (true) {
					val = queue.poll(1, TimeUnit.MINUTES);
					if(val != null) {
						RedissonAdapter.logger.info("{}", val);
						countDownLatch.countDown();
					}
				}
			});
		}

		countDownLatch.await();
	}

}
