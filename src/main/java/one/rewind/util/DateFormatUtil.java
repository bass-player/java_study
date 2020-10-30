package one.rewind.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateFormatUtil {

	public static DateTimeFormatter dfd = DateTimeFormat.forPattern("yyyy-MM-dd");
	public static DateTimeFormatter dff = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	public static DateTimeFormatter dfff = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
	public static DateTimeFormatter dfm = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");
	public static DateTimeFormatter dft = DateTimeFormat.forPattern("HH:mm:ss");
	public static DateTimeFormatter dft1 = DateTimeFormat.forPattern("HH:mm");
	public static DateTimeFormatter dfn = DateTimeFormat.forPattern("yyyyMMdd");
	public static DateTimeFormatter dfn1 = DateTimeFormat.forPattern("dd-MM-yyyy");
	
	public static DateTimeFormatter dfd_en_1 = DateTimeFormat.forPattern("MMM dd, yyyy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_2 = DateTimeFormat.forPattern("MMM dd, yy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_11 = DateTimeFormat.forPattern("dd MMM, yyyy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_21 = DateTimeFormat.forPattern("dd MMM, yy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_12 = DateTimeFormat.forPattern("dd MMM yyyy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_22 = DateTimeFormat.forPattern("dd MMM yy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_3 = DateTimeFormat.forPattern("MMM dd yyyy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_4 = DateTimeFormat.forPattern("MMM dd yy").withLocale(Locale.US);
	
	public static DateTimeFormatter dfd_en_5 = DateTimeFormat.forPattern("dd-MMM-yy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_51 = DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.US);
	
	public static DateTimeFormatter dfd_en_6 = DateTimeFormat.forPattern("MM-yyyy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_61 = DateTimeFormat.forPattern("MMM yyyy").withLocale(Locale.US);
	public static DateTimeFormatter dfd_en_62 = DateTimeFormat.forPattern("yyyy MMM").withLocale(Locale.US);

	/**
	 *
	 * @param pattern
	 * @param locale
	 * @param dateStr
	 * @return
	 */
	public static Date parseTime(String pattern, Locale locale, String dateStr) {
		return DateTimeFormat.forPattern(pattern).withLocale(locale).parseDateTime(dateStr).toDate();
	}

	/**
	 * 一般性日期字符串解析方法
	 * @param in 日期字串
	 * @return Date类型日期
	 * @throws ParseException
	 */
	@SuppressWarnings("deprecation")
	public static Date parseTime(String in) {

		if (in == null) {
			return new Date();
		}
		in = in.trim();

		// 解析前缀
		// 生成偏移量
		Pair<String, Long> shift = getShiftValue(in);

		in = shift.getLeft();
		long shiftValue = shift.getRight();

		Date date = new Date();
		String yyyyMMdd = Calendar.getInstance().get(Calendar.YEAR) + "-" + (Calendar.getInstance().get(Calendar.MONTH)+1) + "-" + Calendar.getInstance().get(Calendar.DATE);
		String thisYear = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
		String lastYear = String.valueOf(Calendar.getInstance().get(Calendar.YEAR) - 1);

		//处理+0800时区问题 更新偏移量
		if(in.contains(":")) {
			Pattern patternTimeZone = Pattern.compile("(?<h>[+-]\\d{2}):?(?<m>\\d{2})$");
			Matcher matcherTimeZone = patternTimeZone.matcher(in);
			if (matcherTimeZone.find()) {
				shiftValue += Integer.parseInt(matcherTimeZone.group("h")) * 60 * 60 * 1000;
				shiftValue += Integer.parseInt(matcherTimeZone.group("m")) * 60 * 1000;
			}
			in = in.replaceAll("(?<h>[+-]\\d{2}):?(?<m>\\d{2})$", "");
		}

		in = in.replaceAll("日", "")
				.replaceAll("[年月]", "-")
				.replaceAll("/", "-")
				.replaceAll("\\.", "-")
				.replaceAll("T", " ")
				.replaceAll("Z", "");

		// 以秒为单位
		if (in.matches("\\d{9,10}")) {
			return new Date(Long.parseLong(in + "000"));
		}
		// 以毫秒为单位
		else if (in.matches("\\d{12,13}")) {
			return new Date(Long.parseLong(in));
		}
		// dd-MM-yyyy
		else if (in.matches("\\d{1,2}-\\d{1,2}-\\d+")) {
			return dfn1.parseDateTime(in).toDate();
		}
		// 默认格式
		else if (in.matches("[A-Za-z]{3,4} \\d{1,2}, \\d{4} \\d{1,2}:\\d{1,2}:\\d{1,2} (AM|PM)")) {
			Date date1 = new Date(in);
			long newtime = date1.getTime() + shiftValue;
			return new Date(newtime);
		}
		// yyyy-MM-dd HH:mm:ss
		else if (in.matches("\\d{2,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
			Date date1 = dff.parseDateTime(in).toDate();
			long newtime = date1.getTime() + shiftValue;
			return new Date(newtime);
		}
		// yyyy-MM-dd HH:mm
		else if (in.matches("\\d{2,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}")) {
			Date date1 = dfm.parseDateTime(in).toDate();
			long newtime = date1.getTime() + shiftValue;
			return new Date(newtime);
		}
		// yyyy-MM-dd
		else if (in.matches("\\d{2,4}-\\d{1,2}-\\d{1,2}")) {
			return dfd.parseDateTime(in).toDate();
		}
		// MM-dd TODO 年份处理
		else if (in.matches("\\d{1,2}-\\d{1,2}")) {

			Date date1 = dfd.parseDateTime(thisYear + '-' + in).toDate();
			if(date1.after(date)) {
				date1 = dfd.parseDateTime(lastYear + '-' + in).toDate();
			}

			return new Date(date1.getTime() + shiftValue);
		}
		// MM-dd HH:mm TODO 年份处理
		else if (in.matches("\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{2}")) {

			Date date1 = dfm.parseDateTime(thisYear + '-' + in).toDate();

			if(date1.after(date)) {
				date1 = dfm.parseDateTime(lastYear + '-' + in).toDate();
			}

			return new Date(date1.getTime() + shiftValue);
		}
		// HH:mm:ss
		else if (in.matches("\\d{1,2}:\\d{2}:\\d{2}")) {

			Date date1 = dff.parseDateTime(yyyyMMdd + " " + in).toDate();
			long newtime = date1.getTime() + shiftValue;
			return new Date(newtime);
		}
		// HH:mm
		else if (in.matches("\\d{1,2}:\\d{2}")) {

			Date date1 = dff.parseDateTime(yyyyMMdd + " " + in  + ":00").toDate();
			long newtime = date1.getTime() + shiftValue;
			return new Date(newtime);
		}
		// yyyyMMdd
		else if (in.matches("\\d{8}")) {

			return dfn.parseDateTime(in).toDate();
		}
		// 英文日期格式 MMM dd, yyyy -- Mar 3, 2016
		else if(in.matches("\\w+ +\\d{1,2} *, +\\d{4}")) {

			in = in.replaceAll(" +,", ",").replaceAll(" +", " ");
			return dfd_en_1.parseDateTime(in).toDate();
		}
		// 英文日期格式 MMM dd, yy -- Mar 3, 16
		else if(in.matches("\\w+ +\\d{1,2} *, +\\d{2}")) {

			in = in.replaceAll(" +,", ",").replaceAll(" +", " ");
			return dfd_en_2.parseDateTime(in).toDate();
		}
		// 英文日期格式dd MMM, yyyy -- Mar 3, 2016
		else if(in.matches("\\d{1,2} +\\w+ *, +\\d{4}")) {

			in = in.replaceAll(" +,", ",").replaceAll(" +", " ");
			return dfd_en_11.parseDateTime(in).toDate();
		}
		// 英文日期格式 dd MMM, yy -- Mar 3, 16
		else if(in.matches("\\d{1,2} +\\w+ *, +\\d{2}")) {

			in = in.replaceAll(" +,", ",").replaceAll(" +", " ");
			return dfd_en_21.parseDateTime(in).toDate();
		}
		// 英文日期格式dd MMM, yyyy -- Mar 3, 2016
		else if(in.matches("\\d{1,2} +\\w+ +\\d{4}")) {

			in = in.replaceAll(" +", " ");
			return dfd_en_12.parseDateTime(in).toDate();
		}
		// 英文日期格式 dd MMM, yy -- Mar 3, 16
		else if(in.matches("\\d{1,2} +\\w+ +\\d{2}")) {

			in = in.replaceAll(" +", " ");
			return dfd_en_22.parseDateTime(in).toDate();
		}
		// 英文日期格式 MMM dd yyyy -- Mar 3, 2016
		else if(in.matches("\\w+ +\\d{1,2} +\\d{4}")) {

			in = in.replaceAll(" +,", ",").replaceAll(" +", " ");
			return dfd_en_3.parseDateTime(in).toDate();
		}
		// 英文日期格式 MMM dd yy -- Mar 3, 16
		else if(in.matches("\\w+ +\\d{1,2} +\\d{2}")) {

			in = in.replaceAll(" +,", ",").replaceAll(" +", " ");
			return dfd_en_4.parseDateTime(in).toDate();
		}
		// 英文日期格式01-Nov-15
		else if(in.matches("\\d{1,2}-\\w+-\\d{2}")) {

			return dfd_en_5.parseDateTime(in).toDate();
		}
		// 英文日期格式01-Nov-2015
		else if(in.matches("\\d{1,2}-\\w+-\\d{4}")) {

			return dfd_en_51.parseDateTime(in).toDate();
		}
		// 英文日期格式10/2016
		else if(in.matches("\\d{1,2}-\\d{4}")) {
			return dfd_en_6.parseDateTime(in).toDate();
		}
		// 英文日期格式June 2016
		else if(in.matches("\\w+ \\d{4}")) {
			return dfd_en_61.parseDateTime(in).toDate();
		}
		// 英文日期格式2016 June
		else if(in.matches("\\d{4} \\w+")) {
			return dfd_en_62.parseDateTime(in).toDate();
		}
		// 英文日期格式21 Oct
		else if(in.matches("\\d{1,2} \\w+")) {

			Date date1 = dfd_en_12.parseDateTime(in + " " + thisYear).toDate();

			if(date1.after(date)) {
				date1 = dfd_en_12.parseDateTime(in + " " + lastYear).toDate();
			}

			return new Date(date1.getTime() + shiftValue);
		}
		// 英文日期格式Oct 21
		else if(in.matches("\\w+ \\d{1,2}")) {

			Date date1 = dfd_en_3.parseDateTime(in + " " + thisYear).toDate();

			if(date1.after(date)) {
				date1 = dfd_en_3.parseDateTime(in + " " + lastYear).toDate();
			}

			return new Date(date1.getTime() + shiftValue);
		}
		else if (shiftValue != 0) {
			return new Date(new Date().getTime() + shiftValue);
		}
		// 不能解析的情况
		else {
			return date;
		}
	}

	/**
	 * 获得文本日期时间描述的偏移量
	 * @param in
	 * @return
	 */
	public static Pair<String, Long> getShiftValue(String in) {

		long v = 0;

		Pattern p = Pattern.compile("((?<x>[今昨前明后][天日])|(?<a>\\d+|[一二三四五六七八九十百]+)个?(?<b>年|月|日|周|星期|天|分钟|小时)(?<c>[前后]))");
		Matcher m = p.matcher(in);

		String out = in;

		if(m.find()) {

			out = in.replaceAll(m.group(), "").trim();

			if(m.group("x") != null) {

				if (m.group("x").matches("昨[天日]")){
					v = - 24 * 60 * 60 * 1000;
				}
				else if (m.group("x").matches("前[天日]")){
					v = - 2 * 24 * 60 * 60 * 1000;
				}
				else if (m.group("x").matches("明[天日]")){
					v = 24L * 60 * 60 * 1000;
				}
				else if (m.group("x").matches("后[天日]")){
					v = 2 * 24 * 60 * 60 * 1000;
				}
			}
			else {

				int a = 0;
				long b = 0;
				if(m.group("a") != null) {

					a = NumberFormatUtil.parseInt(m.group("a"));
				}

				if(m.group("b") != null) {

					if(m.group("b").equals("年")) {
						b = 356L * 24 * 60 * 60 * 1000;
					}
					else if(m.group("b").equals("月")) {
						b = 30L * 24 * 60 * 60 * 1000;
					}
					else if(m.group("b").matches("周|星期")) {
						b = 7L * 24 * 60 * 60 * 1000;
					}
					else if(m.group("b").matches("[日天]")) {
						b = 24L * 60 * 60 * 1000;
					}
					else if(m.group("b").equals("小时")) {
						b = 60L * 60 * 1000;
					}
					else if(m.group("b").equals("分钟")) {
						b = 60L * 1000;
					}
				}

				if(m.group("c") != null) {
					if(m.group("c").equals("前")) {
						v = b * a * -1;
					}
					else if(m.group("c").equals("后")) {
						v = b * a;
					}
				}
			}
		}

		return new ImmutablePair<>(out, v);
	}

	/**
	 *
	 * @param date
	 * @param val
	 * @return
	 */
	public static Date modify(Date date, int val) {
		return new Date(date.getTime() + val * 3600 * 1000);
	}

	/**
	 *
	 * @param x
	 * @return
	 */
	public static byte[] longToBytes(long x) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(x);
		return buffer.array();
	}
}
