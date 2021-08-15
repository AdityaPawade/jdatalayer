package com.adtsw.jdatalayer.core.client;

import com.adtsw.jcommons.utils.CompressionUtil;
import com.adtsw.jdatalayer.core.model.StorageFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public abstract class AbstractDBClient implements IDBClient {

    protected static Logger logger = LogManager.getLogger(AbstractDBClient.class);

    protected String encode(StorageFormat storageFormat, String payload) {
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

    protected String decode(StorageFormat storageFormat, String storedPayload) {
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
