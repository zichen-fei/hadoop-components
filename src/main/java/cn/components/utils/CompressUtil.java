package cn.components.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class CompressUtil {

    private static String charsetName = "UTF-8";

    /**
     * 压缩
     * @param message string
     * @return byte[]
     */
    public static byte[] compress(String message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream dos = new DeflaterOutputStream(baos);
        dos.write(message.getBytes(charsetName));
        dos.close();
        return baos.toByteArray();
    }

    /**
     * 解压缩
     * @param message byte[]
     * @return String
     */
    public static String uncompress(byte[] message) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(message);
        InflaterInputStream ii = new InflaterInputStream(bis);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        int c;
        byte[] buf = new byte[2048];
        while (true) {
            c = ii.read(buf);
            if (c == -1) {
                break;
            }
            baos.write(buf, 0, c);
        }
        ii.close();
        baos.close();
        return new String(baos.toByteArray(), charsetName);
    }
}
