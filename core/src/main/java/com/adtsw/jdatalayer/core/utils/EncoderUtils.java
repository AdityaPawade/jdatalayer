package com.adtsw.jdatalayer.core.utils;

import com.adtsw.jcommons.utils.CompressionUtil;
import com.adtsw.jdatalayer.core.model.StorageFormat;

import java.io.IOException;

public class EncoderUtils {

    public static String encode(StorageFormat storageFormat, String payload) {
        String encodedPayload = null;
        switch (storageFormat) {
            case STRING:
                encodedPayload = payload;
                break;
            case GZIP_WITH_BASE64:
                try {
                    encodedPayload = CompressionUtil.compress(payload);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage());
                }
                break;
        }
        return encodedPayload;
    }

    public static String decode(StorageFormat storageFormat, String storedPayload) {
        String decodedPayload = null;
        switch (storageFormat) {
            case STRING:
                decodedPayload = storedPayload;
                break;
            case GZIP_WITH_BASE64:
                try {
                    decodedPayload = CompressionUtil.decompress(storedPayload);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage());
                }
                break;
        }
        return decodedPayload;
    }
}
