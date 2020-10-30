/**
 *
 */
package one.rewind.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 字符串常用工具类
 * @author scisaga@gmail.com
 * @date 2016.7.20
 */
public class StringUtil {

	/**
	 * 清空已有的文件内容，若原文件不存在，则创建
	 * @param fileName
	 */
	public static void clearInfoOfFile(String fileName) {
		//A.删除原文件，创建新文件
		//B.写入空字符串到文件中
		//C.文件不存在时，自动创建
		/*PrintWriter pw = new PrintWriter(fileName);
		pw.close();*/

		//D.文件不存在时，NoSuchFileException
		try {
			FileChannel.open(Paths.get(fileName), StandardOpenOption.WRITE).truncate(0).close();
		}catch (Exception e){
			return;
		}
	}

	/**
	 * 对长出的字符串进行截断
	 */
	public static String strCrop(String in, int length) {
		if (in != null && in.length() > length) {
			return in.substring(0, length);
		} else
			return in;
	}
	
	/**
	 * 根据spliter第一次出现的位置
	 * 将输入字符串一分为二
	 * @param src
	 * @param spliter
	 * @return
	 */
	public static String[] splitFirstChar(String src, String spliter) {

		String[] res = new String[2];
		int eindex = src.indexOf(spliter);
		if (eindex < 0) {
			return null;
		}
		int slen = spliter.length();

		res[0] = src.substring(0, eindex);
		res[1] = src.substring(eindex + slen);
		return res;

	}
	
	/**
	 * 清理Ctrl-Char和反斜杠
	 *
	 * @param in
	 * @return
	 */
	public static String removeCtrlChars(String in) {
		String out = in;

		// 处理反斜杠
		out = out.replaceAll("\\\\", "\\\\\\\\");
		out = out.replaceAll("\\\\\\\"", "\\\\\\\\\"");
		out = out.replaceAll(" | ", "");

		out = out.replace("\n|\b|\f|\t|\r", "");

		// ascii 0-31 ctrl-char + ascii 127 del
		out = out.replaceAll("[\u0000-\u001f\u007f]", "");

		return out;
	}

	/**
	 * 去除HTML标记
	 *
	 * @param in
	 * @return
	 */
	public static String removeHTML(String in) {
		return in.replaceAll("<[^>]+?>", "");
	}

	/**
	 * 去除空格符
	 *
	 * @param in
	 * @return
	 */
	public static String removeBlank(String in) {
		return in.replaceAll("\\s*", "");
	}

	/**
	 * 去掉多余的空格和HTML标记
	 *
	 * @param in
	 * @return
	 */
	public static String purgeHTML(String in) {
		if (in == null) {
			return null;
		}

		return in.replaceAll("^[ |	|　]+", "").replaceAll("[ |	|　]+$", "")
				.replaceAll(">[ |	|　]+", ">").replaceAll("[ |	|　]+<", "<")
				.replaceAll("&nbsp;", "").replaceAll("style=\".*?\"", "");
	}

	/**
	 * 生成随机uuid
	 * 32位字符串
	 * @return
	 */
	public static String uuid() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

	/**
	 *
	 * @param src
	 * @return
	 */
	public static String md5(String src) {

		return byteArrayToHex(md5B(src.getBytes()));
	}

	/**
	 *
	 * @return
	 */
	public static byte[] md5B(String src) {

		return md5B(src.getBytes());
	}

	/**
	 *
	 * @param src
	 * @return
	 */
	public static byte[] md5B(byte[] src) {

		MessageDigest m;
		try {
			m = MessageDigest.getInstance("MD5");

			m.reset();
			m.update(src);
			return m.digest();
		}
		catch (NoSuchAlgorithmException e) {
			return hexStringToByteArray(uuid());
		}
	}

	/**
	 * 将UUID转换为bin(16)
	 *
	 * @param uuid
	 * @return
	 */
	public static byte[] getIdAsByte(UUID uuid) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}

	/**
	 * 将bin(16)转换为UUID NOTE: 转换不是互逆的
	 *
	 * @param bytes
	 * @return
	 */
	public static UUID toUUID(byte[] bytes) {

		if (bytes.length != 16) {
			throw new IllegalArgumentException();
		}
		int i = 0;
		long msl = 0;
		for (; i < 8; i++) {
			msl = msl << 8 | bytes[i] & 0xFF;
		}
		long lsl = 0;
		for (; i < 16; i++) {
			lsl = lsl << 8 | bytes[i] & 0xFF;
		}
		return new UUID(msl, lsl);
	}

	/**
	 * 将byte[]转化为十进制字符串
	 *
	 * @param a
	 * @return
	 */
	public static String byteArrayToHex(byte[] a) {

		if (a == null) return "";

		StringBuilder sb = new StringBuilder(a.length * 2);

		for (byte b : a) {
			sb.append(String.format("%02x", b & 0xff));
		}

		return sb.toString();
	}

	/**
	 * 将十进制字符串转化为Byte[]
	 *
	 * @param s
	 * @return
	 */
	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		if (len%2 == 1){
			throw new IllegalArgumentException("String length can't be odd!");
		}
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character
					.digit(s.charAt(i + 1),
				 			16));
		}
		return data;
	}

	/**
	 * 转换成UTF-8
	 * 
	 * @param str
	 * @return
	 */
	public static String toUTF8(String str) {
		String result = str;
		try {
			result = new String(str.getBytes(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * MD5 encode
	 * @param inStr
	 * @return
	 */
	public static String MD5(String inStr) {
		return StringUtil.byteArrayToHex(StringUtil.md5B(inStr));
	}

	/**
	 *
	 * @param src
	 * @return
	 */
	public static String MD5(byte[] src) {
		return StringUtil.byteArrayToHex(StringUtil.md5B(src));
	}

	/**
	 *
	 */
	public static String cronPattern = "(\\*|((\\*\\/)?[1-5]?[0-9])) (\\*|((\\*\\/)?(1?[0-9]|2[0-3]))) (\\*|((\\*\\/)?([1-9]|[12][0-9]|3[0-1]))) (\\*|((\\*\\/)?([1-9]|1[0-2]))) (\\*|((\\*\\/)?[0-6]))";

	/**
	 * Validate cron pattern
	 * @param cron
	 * @return
	 */
	public static boolean validCron(String cron) {

		if(cron.matches(cronPattern)) {
			return true;
		}

		return false;
	}

	/**
	 *
	 */
	public static final class AlphabetEncoder {

		private static final char[] ALPHABET = new char[64];
		private static final long ENCODE_LENGTH;

		static {

			ALPHABET[0] = 45;
			for(int i=1; i<11; i++) {
				ALPHABET[i] = (char) (i + 47);
			}
			for(int i=11; i<37; i++) {
				ALPHABET[i] = (char) (i + 53);
			}
			for(int i=37; i<64; i++) {
				ALPHABET[i] = (char) (i + 60);
			}

			ENCODE_LENGTH = ALPHABET.length;
		}

		public static String encode(long victim)
		{
			final List<Character> list = new ArrayList<>();

			do {
				list.add(ALPHABET[(int) (victim % ENCODE_LENGTH)]);
				victim /= ENCODE_LENGTH;
			} while (victim > 0);

			Collections.reverse(list);
			return list.stream().map(String::valueOf).collect(Collectors.joining());
		}

		public static long decode(final String encoded)
		{
			long ret = 0;
			char c;
			for (int index = 0; index < encoded.length(); index++) {
				c = encoded.charAt(index);
				ret *= ENCODE_LENGTH;
				ret += Arrays.binarySearch(ALPHABET, c);
			}
			return ret;
		}
	}

	/**
	 * 将16字节MD5哈希值转换为8字节
	 * 方法是使用最低有效位（最后64位）执行xor最高有效位（第一个64位）
	 * 目的只是为了减少生成的数据的大小，并使其与在线代码兼容
	 * @return
	 */
	public static byte[] convert16To8ByteXor(final byte[] byteToConvert16){
		byte[] xored8Byte = new byte[8];

		//xor msb(1st 8 byte) and lsb (last 8 byte)
		for( int i = 0; i < byteToConvert16.length/2; i++){
			xored8Byte[i] = (byte) (byteToConvert16[i] ^ byteToConvert16[i+8]);
		}

		return xored8Byte;
	}
}
