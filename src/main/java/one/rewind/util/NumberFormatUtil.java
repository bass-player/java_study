package one.rewind.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NumberFormatUtil {
	/**
	 *
	 * @param in
	 * @return
	 */
	public static double parseDouble(String in) {

		double v = 0;

		Map<String, Double> map = new HashMap<>();

		map.put("百", 100D);
		map.put("千", 1000D);
		map.put("万", 10000D);
		map.put("百万", 100 * 10000D);
		map.put("亿", 10000 * 10000D);
		map.put("K", 1000D);
		map.put("k", 1000D);
		map.put("M", 1000000D);
		map.put("m", 1000000D);
		map.put("G", 1000000000D);
		map.put("g", 1000000000D);
		map.put("T", 1000000000000D);
		map.put("t", 1000000000000D);

		List<Double> multis = new ArrayList<>();
		Pattern p = Pattern.compile("百|千|万|百万|亿|k|K|m|M|g|G|t|T");
		Matcher m = p.matcher(in);
		while(m.find()){
			multis.add(map.get(m.group()));
		}

		in = in.trim();
		boolean negative = false;
		if(in.length() > 1 && in.subSequence(0, 1).equals("-")) {
			negative = true;
		}

		in = in.replaceAll("百|千|万|百万|亿|k|K|m|M|g|G|t|T|,", "").replaceAll("\\+|-", "").trim();

		if(in.matches("(\\d+\\.)?\\d+")){
			v = Double.parseDouble(in);
			for(Double ms : multis){
				v *= ms;
			}
		}
		else if (in.matches("[一二三四五六七八九十]")) {

			switch (in) {
				case "一" : v = 1; break;
				case "二" : v = 2; break;
				case "三" : v = 3; break;
				case "四" : v = 4; break;
				case "五" : v = 5; break;
				case "六" : v = 6; break;
				case "七" : v = 7; break;
				case "八" : v = 8; break;
				case "九" : v = 9; break;
				case "十" : v = 10; break;
			}
		}

		return negative ? -v : v;
	}

	/**
	 *
	 * @param in
	 * @return
	 */
	public static float parseFloat(String in){
		return (float) parseDouble(in);
	}

	/**
	 *
	 * @param in
	 * @return
	 */
	public static int parseInt(String in){
		return (int) parseDouble(in);
	}

	private static final String[] KMG = new String[] {"", "K", "M", "G"};

	/**
	 *
	 * @param d
	 * @return
	 */
	public static String f2(double d) {
		int i = 0;
		while (d >= 1000) { i++; d /= 1000; }
		return String.format("%.2f", d) + KMG[i];
	}
}
