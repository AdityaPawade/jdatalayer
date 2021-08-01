package com.adtsw.jcommons.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;

public class UrlUtil {

    public static HashMap<String, String> getQueryParams(String message) {
        HashMap<String, String> params = new HashMap<>();
        if(StringUtils.isNotEmpty(message)) {
            String[] splits = message.split("\\?");
            if(splits.length > 1) {
                String[] queryParams = splits[1].split("&");
                if(queryParams.length > 0) {
                    for (String queryParam : queryParams) {
                        String[] paramValue = queryParam.split("=");
                        params.put(paramValue[0], paramValue[1]);
                    }
                }
            }
        }
        return params;
    }
}
