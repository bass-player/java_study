package one.rewind.util;

import one.rewind.db.model.Model;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * 文件工具类
 * @author scisaga@gmail.com
 * @date 2015.3.7
 */
public class FileUtil {

	public static final Logger logger = LogManager.getLogger(FileUtil.class.getName());

	public interface LineCallback {
		void run(String line);
	}

	/**
	 * 逐行遍历 执行行内容的回调操作
	 * @param fileName
	 * @param callback
	 */
	public static void traversalByLines(String fileName, LineCallback callback) {

		File file = new File(fileName);

		if(file.exists()){

			BufferedReader reader = null;

			try {
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				while ((tempString = reader.readLine()) != null) {
					callback.run(tempString);
				}
			}
			catch (IOException e) {
				logger.error("Error read from {}", fileName, e);
			}
			finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
						logger.error("Error close {}", fileName, e1);
					}
				}
			}
		}
	}

	/**
	 * 逐行读取文件
	 * @param fileName
	 * @return
	 */
	public static String readFileByLines(String fileName) {

		String output = "";

		File file = new File(fileName);

		if(file.exists()){

			BufferedReader reader = null;

			try {
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				while ((tempString = reader.readLine()) != null) {
					output += tempString + "\n";
				}
			}
			catch (IOException e) {
				logger.error("Error read from {}", fileName, e);
			}
			finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
						logger.error("Error close {}", fileName, e1);
					}
				}
			}

		}
		else {

			InputStreamReader reader = null;

			InputStream stream = FileUtil.class.getClassLoader().getResourceAsStream(fileName);

			if(stream == null) return null;

			try {
				reader = new InputStreamReader(stream);

				int tmp;
				while((tmp = reader.read()) != -1){
					output += (char)tmp;
				}
			}
			catch (IOException e) {
				logger.error("Error read from {}", fileName, e);
			}
			finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
						logger.error("Error close {}", fileName, e1);
					}
				}
			}
		}

		return output;
	}
	
	/**
	 * 读取文件
	 * @param fileName
	 * @return
	 */
	public static byte[] readBytesFromFile(String fileName) {
		File file = new File(fileName);
		return readBytesFromFile(file);
	}

	/**
	 * 读取文件
	 * @param file
	 * @return
	 */
	public static byte[] readBytesFromFile(File file) {

		try {
			FileInputStream fin = new FileInputStream(file);
			ByteBuffer nbf = ByteBuffer.allocate((int) file.length());
			byte[] array = new byte[1024];
			int offset = 0, length = 0;
			while ((length = fin.read(array)) > 0) {
				if (length != 1024)
					nbf.put(array, 0, length);
				else
					nbf.put(array);
				offset += length;
			}
			fin.close();
			byte[] content = nbf.array();
			return content;
		}
		catch (FileNotFoundException e) {
			logger.error("File not found, ", e);
		}
		catch (IOException e) {
			logger.error("Error read from {}", file.getPath(), e);
		}

		return null;
	}
	
	/**
	 * 将byte数组写入文件
	 * TODO: 当文件较大时候可能发生问题
	 * @param fileName
	 * @param fileBytes
	 * @return
	 */
	public static boolean writeBytesToFile(byte[] fileBytes, String fileName) {
		
		try {

			// 文件夹不存在创建文件夹
			if(fileName.contains("/|\\")) {

				String folder_path = fileName.replaceAll("/[^/]+?$", "")
						.replaceAll("\\[^\\]+?$", "");

				if (folder_path.length() > 0) {

					File directory = new File(folder_path);
					if (!directory.exists()) {
						directory.mkdir();
					}
				}
			}

			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fileName));
			bos.write(fileBytes);
			bos.flush();
			bos.close();
			return true;
			
		}
		catch (FileNotFoundException e) {
			logger.error("File not found, ", e);
		}
		catch (IOException e) {
			logger.error("Error read from {}", fileName, e);
		}

		return false;
	}

	/**
	 * 向文件末尾追加行
	 * @param line
	 * @param fileName
	 * @return
	 */
	public static boolean appendLineToFile(String line, String fileName) {

		// 文件夹不存在创建文件夹
		if(fileName.contains("/")) {

			String folder_path = fileName.replaceAll("/[^/]+?$", "");

			if (folder_path.length() > 0) {

				File directory = new File(folder_path);
				if (!directory.exists()) {
					directory.mkdir();
				}
			}
		}

		try {

			BufferedWriter output = new BufferedWriter(new FileWriter(fileName, true));
			output.write(line);
			output.newLine();
			output.close();
			return true;

		} catch (IOException e) {
			logger.error("Write to {} error, ", fileName, e);
		}

		return false;
	}

	/**
	 * 从文件中读取序列化的Java对象
	 *
	 * @param serPath
	 * @return Java对象
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static <T> T load(String serPath) throws IOException, ClassNotFoundException {

		FileInputStream fileIn = new FileInputStream(serPath);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object o = in.readObject();
		in.close();
		fileIn.close();
		return (T) o;
	}

	/**
	 * 将Java对象序列化到文件中
	 * @param o
	 * @param serPath
	 * @throws IOException
	 */
	public static <T> void serialize(T o, String serPath) throws IOException {

		FileOutputStream fileOut = new FileOutputStream(serPath);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(o);
		out.close();
		fileOut.close();
		Model.logger.info("Serialized {}} into {}.", o.getClass().getSimpleName(), serPath);
	}
}
