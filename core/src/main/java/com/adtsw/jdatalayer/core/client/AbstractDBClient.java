package com.adtsw.jdatalayer.core.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractDBClient implements IDBClient {

    protected static Logger logger = LogManager.getLogger(AbstractDBClient.class);
}
