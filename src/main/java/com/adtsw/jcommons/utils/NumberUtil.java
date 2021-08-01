package com.adtsw.jcommons.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class NumberUtil {

    public static double round(double input, int scale, RoundingMode mode) {
        return new BigDecimal(input).setScale(scale, mode).doubleValue();
    }

    public static int getNumDecimals(Double value) {
        String[] splitter = value.toString().split("\\.");
        return splitter[1].length();
    }
}
