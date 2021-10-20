package com.streamnative.cep.common.codegen.utils;

import com.streamnative.cep.common.codegen.GenerateCode;
import com.streamnative.cep.common.entity.Rule;

public class CodeGenGenerateUtils {

    public static GenerateCode generateCode(Rule rule) {
        Integer ruleId = rule.getRuleId();
        String mainClass = String.format(
                "package com.streamnative.cep.common.codegen;" +
                        "import com.streamnative.cep.common.codegen.FieldExtractorFunction;\n" +
                        "\n" +
                        "\n" +
                        "public class DynamicFieldExtractorFunction%s extends FieldExtractorFunction {\n" +
                        "\n" +
                        "    public DynamicFieldExtractorFunction%s(Integer ruleId) {\n" +
                        "        super(%s);\n" +
                        "    }\n" +
                        "\n" +
                        "    public DynamicFieldExtractorFunction%s() {" +
                        "setRuleId(%s);\n" +
                        "    }\n" +
                        "}", ruleId, ruleId, ruleId, ruleId, ruleId);
        String pckName = "com.streamnative.cep.common.codegen.DynamicFieldExtractorFunction" + ruleId;
        GenerateCode generateCode = new GenerateCode(pckName, mainClass);
        return generateCode;
    }
}
