package red.reid.common;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.*;

/**
 * https://github.com/perwendel/spark
 */
public class WebAPIServer {

	private static final Logger logger = LogManager.getLogger(WebAPIServer.class.getName());

	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public WebAPIServer() {
		port(10000);
	}

	/**
	 * https://sparkjava.com/documentation#routes
	 */
	public void buildRoutes() {

		before("/*", (q, a) -> logger.info("Received api call"));
		get("/hello", (q, a) -> "Hello World");
		get("/user/:id", (q, a) -> {

			String id = q.params("id");
			String query = q.queryParams("query");

			a.type("application/json");
			return gson.toJson(ImmutableMap.of("id", id, "query", query));

		});

	}

	/**
	 * http://127.0.0.1:10000/user/111?query=1212
	 * @param args
	 */
	public static void main(String[] args) {

		new WebAPIServer().buildRoutes();
	}
}
