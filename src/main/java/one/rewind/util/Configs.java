package one.rewind.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配置文件适配器
 * 单独为每个类进行配置
 * @author scisaga@gmail.com
 * @date 2017.11.12
 */
public class Configs {

	private static final Config base = ConfigFactory.load();
	private static final Config dev;
	private static final Config test;
	private static final Config prod;

	private static Map<String, Config> configs = new ConcurrentHashMap<>();

	static {
		dev = load("conf/dev.conf").withFallback(base);
		test = load("conf/test.conf").withFallback(dev);
		prod = load("conf/prod.conf").withFallback(test);
	}

	/**
	 *
	 * @param path
	 * @return
	 */
	private static Config load(String path) {

		Config config;
		File file = new File(path);

		if(file.exists()){
			config = ConfigFactory.parseFile(file).withFallback(base);
		}
		else {
			InputStream stream = Configs.class.getClassLoader().getResourceAsStream(path);
			if(stream != null) {
				config = ConfigFactory.parseReader(new InputStreamReader(stream)).withFallback(base);
			}
			else {
				config = base;
			}
		}
		return config;
	}

	/**
	 * 基于 Class 单独定义每个类的配置
	 * 配置文件在conf/下
	 * 单独配置文件名与类名相同
	 * @param clazz
	 * @return
	 */
	public static Config getConfig(Class<?> clazz) {

		String key = clazz.getSimpleName();

		try {
			return prod.getConfig(key);
		}
		catch (Exception e) {
			return configs.computeIfAbsent(key, v -> load("conf/" + key + ".conf"));
		}
	}

	/**
	 * 整合开发配置文件
	 * @param name
	 */
	public static void export(String name) throws IOException {

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode conf = mapper.createObjectNode();

		for(File f : Objects.requireNonNull(new File("src/main/resources/conf").listFiles())) {
			if(!f.isDirectory() && !f.getName().matches("(dev|test|prod).*?")) {

				JsonNode node = mapper.readTree(FileUtil.readFileByLines(f.getAbsolutePath()));
				conf.set(f.getName().replaceAll("\\..+?$", ""), node);
			}
		}

		FileUtil.writeBytesToFile(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(conf).getBytes(),
				"src/main/resources/conf/" + name + ".conf");

	}
}
