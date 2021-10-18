package com.adtsw.jdatalayer.core.annotations;

import com.adtsw.jcommons.models.EncodingFormat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DBEntityConfiguration {

    String setName();

    EncodingFormat encodingFormat();
}
