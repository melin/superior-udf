package com.dataworker.udf.ac;

import org.ahocorasick.trie.Trie;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

/**
 * @author melin 2021/8/12 4:34 下午
 */
public class TextContainsMatchUDF extends GenericUDF {
    static final Logger LOG = LoggerFactory.getLogger(TextContainsMatchUDF.class.getName());

    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
    private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];
    private final BooleanWritable output = new BooleanWritable();
    private transient boolean isRegexConst;
    private transient String regexConst;
    private transient Trie trieConst;
    private transient boolean warned;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 2, 2);

        checkArgPrimitive(arguments, 0);
        checkArgPrimitive(arguments, 1);

        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        checkArgGroups(arguments, 1, inputTypes, STRING_GROUP);

        obtainStringConverter(arguments, 0, inputTypes, converters);
        obtainStringConverter(arguments, 1, inputTypes, converters);

        if (arguments[1] instanceof ConstantObjectInspector) {
            regexConst = getConstantStringValue(arguments, 1);
            if (regexConst != null) {
                String[] words = StringUtils.split(regexConst, "|");
                Trie.TrieBuilder builder = Trie.builder();
                for (String word : words) {
                    builder.addKeyword(word);
                }
                trieConst = builder.build();
            }
            isRegexConst = true;
        }

        ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String text = getStringValue(arguments, 0, converters);
        if (text == null) {
            return null;
        }

        String regex;
        if (isRegexConst) {
            regex = regexConst;
        } else {
            regex = getStringValue(arguments, 1, converters);
        }
        if (regex == null) {
            return null;
        }

        if (regex.length() == 0) {
            if (!warned) {
                warned = true;
                LOG.warn(getClass().getSimpleName() + " regex is empty. Additional "
                        + "warnings for an empty regex will be suppressed.");
            }
            output.set(false);
            return output;
        }

        Trie trie;
        if (isRegexConst) {
            trie = trieConst;
        } else {
            String[] words = StringUtils.split(regex, "|");
            Trie.TrieBuilder builder = Trie.builder();
            for (String word : words) {
                builder.addKeyword(word);
            }
            trie = builder.build();
        }

        boolean result = trie.containsMatch(text);
        output.set(result);
        return output;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "ac_contains_match(" + children[0] + " , " + children[1] + ")";
    }

    @Override
    protected String getFuncName() {
        return "ac_contains_match";
    }
}
