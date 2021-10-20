package com.streamnative.cep.common.codegen.utils;

import com.streamnative.cep.common.codegen.FieldExtractorFunction;
import com.streamnative.cep.common.codegen.GenerateCode;
import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Rule;
import org.apache.flink.api.common.InvalidProgramException;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DynamicCodeCompileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicCodeCompileUtils.class);

    private static <T> T compile(ClassLoader cl, String name, String code) {
        checkNotNull(cl, "Classloader must not be null.");
        LOG.info("Compiling: {} \n\n Code:\n{}", name, code);
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(cl);
        try {
            compiler.cook(code);
        } catch (Throwable t) {
            throw new InvalidProgramException(
                    "program cannot be compiled. This is a bug. Please file an issue.", t);
        }
        try {
            return (T) compiler.getClassLoader()
                    .loadClass(name).newInstance();
        } catch (Throwable e) {
            throw new RuntimeException("Can not load class " + name, e);
        }
    }

}
