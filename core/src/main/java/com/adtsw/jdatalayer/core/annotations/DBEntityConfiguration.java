package com.adtsw.jdatalayer.core.annotations;

import com.adtsw.jdatalayer.core.model.StorageFormat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DBEntityConfiguration {

    String setName();

    StorageFormat storageFormat();
}
