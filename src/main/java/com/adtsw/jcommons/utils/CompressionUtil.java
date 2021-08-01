package com.adtsw.jcommons.utils;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtil {

    public static String compress(String string)
        throws IOException {
        ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
        GZIPOutputStream zos = new GZIPOutputStream(rstBao);
        zos.write(string.getBytes());
        IOUtils.closeQuietly(zos);

        byte[] bytes = rstBao.toByteArray();
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static String decompress(String zippedBase64Str)
        throws IOException {
        String result = null;

        byte[] bytes = Base64.getDecoder().decode(zippedBase64Str);
        GZIPInputStream zi = null;
        try {
            zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
            result = IOUtils.toString(zi, StandardCharsets.UTF_8);
        } finally {
            IOUtils.closeQuietly(zi);
        }
        return result;
    }
}
