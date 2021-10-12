package com.adtsw.jdatalayer.core.utils;

import com.adtsw.jdatalayer.core.model.StorageFormat;
import org.junit.Assert;
import org.junit.Test;

public class EncoderUtilsTest {

    private final String GZIP_ENCODED_STRING = "H4sIAAAAAAAAACsuKcrMSwcAqbK+ngYAAAA=";
    private final String BASIC_ENCODED_STRING = "string";
    private final String DECODED_STRING = "string";

    @Test
    public void testEncodeGZipSuccess() {
        String encodedString = EncoderUtils.encode(StorageFormat.GZIP_WITH_BASE64, DECODED_STRING);
        Assert.assertEquals(GZIP_ENCODED_STRING, encodedString);
    }

    @Test
    public void testEncodeStringSuccess() {
        String encodedString = EncoderUtils.encode(StorageFormat.STRING, DECODED_STRING);
        Assert.assertEquals(BASIC_ENCODED_STRING, encodedString);
    }

    @Test
    public void testDecodeGZipSuccess() {
        String decodedString = EncoderUtils.decode(StorageFormat.GZIP_WITH_BASE64, GZIP_ENCODED_STRING);
        Assert.assertEquals(DECODED_STRING, decodedString);
    }

    @Test
    public void testDecodeSuccess() {
        String decodedString = EncoderUtils.decode(StorageFormat.STRING, BASIC_ENCODED_STRING);
        Assert.assertEquals(DECODED_STRING, decodedString);
    }
}
