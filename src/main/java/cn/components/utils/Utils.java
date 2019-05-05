package cn.components.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {

    public static String relativelyPath = System.getProperty("user.dir");

    public static String base_path = relativelyPath + "/src";

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    /**
     * message转map
     *
     * @param message json串
     * @return Map<String  ,  String>
     */
    public static Map<String, String> processMessageToMap(String message) {
        message = message.split("\\{")[1];
        message = message.split("}")[0];

        Map<String, String> map = new HashMap<>(16);
        String[] keyValues = message.split(",");
        for (String kv : keyValues) {
            String[] fields = kv.split(":");
            if (fields.length == 1) {
                map.put(StringUtils.trim(fields[0]).replaceAll("\"", ""), "");
            } else {
                map.put(StringUtils.trim(fields[0]).replaceAll("\"", ""), StringUtils.trim(fields[1]).replaceAll("\"", ""));
            }
        }
        return map;
    }

    /**
     * 字符串时间转换成UTC时间
     *
     * @param time 20150717173636
     * @return long  毫秒值
     */
    public static long convertToUTC(String time) {
        try {
            if (StringUtils.isBlank(time)) {
                return 0L;
            }
            if (time.length() == "150714140400".length()) {
                time = "20" + time;
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            Date date = simpleDateFormat.parse(time);
            return date.getTime();
        } catch (Exception e) {
            LOGGER.error("Error in time format!===>{}", time);
            return 0L;
        }
    }

    /**
     * 字符串时间转换成timestamp
     *
     * @param time 2018-09-29 13:12:01
     * @return long  毫秒值
     */
    public static long convertToTimestamp(String time) {
        try {
            if (StringUtils.isBlank(time)) {
                return 0L;
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = simpleDateFormat.parse(time);
            return date.getTime();
        } catch (Exception e) {
            LOGGER.error("Error in time format!===>{}", time);
            return 0L;
        }
    }

    /**
     * 读取文件
     *
     * @param path 文件路径
     * @return List<String></>
     */
    public static List<String> readFile(String path) {
        List<String> datas = new ArrayList<>();
        try {
            File filename = new File(path);
            InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
            BufferedReader br = new BufferedReader(reader);
            String line;
            line = br.readLine();
            while (line != null) {
                datas.add(line);
                line = br.readLine();
            }
            br.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datas;
    }
}
