package org.pea;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.common.config.ConfigDef;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class HyphenToUnderscore implements Transformation<SourceRecord> {
    @Override
    public SourceRecord apply(SourceRecord record) {
        Object value = record.value();
        Schema schema = record.valueSchema();
        if (value == null) {
            return record;
        }

        if (schema == null && value instanceof Map<?, ?> mapValue) {
            boolean allStringKeys = mapValue.keySet().stream().allMatch(k -> k instanceof String);
            if (allStringKeys) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stringMap = (Map<String, Object>) mapValue;
                Object newValue = processMap(stringMap);
                SourceRecord newRecord = record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    newValue,
                    record.timestamp()
                );
                return newRecord;
            } else {
                return record;
            }
        } else if (value instanceof Struct structValue) {
            Schema newSchema = transformSchema(schema);
            Struct newStruct = processStruct(structValue, schema);
            SourceRecord newRecord = record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newStruct,
                record.timestamp()
            );
            return newRecord;
        } else {
            return record;
        }
    }

    private Map<String, Object> processMap(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String newKey = entry.getKey().replace("-", "_");
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value);
            } else if (value instanceof Iterable) {
                value = processIterable((Iterable<?>) value);
            }
            result.put(newKey, value);
        }
        return result;
    }

    private Object processIterable(Iterable<?> iterable) {
        java.util.List<Object> result = new java.util.ArrayList<>();
        for (Object item : iterable) {
            if (item instanceof Map) {
                result.add(processMap((Map<String, Object>) item));
            } else if (item instanceof Iterable) {
                result.add(processIterable((Iterable<?>) item));
            } else {
                result.add(item);
            }
        }
        return result;
    }

    private Schema transformSchema(Schema schema) {
        if (schema.type() != Schema.Type.STRUCT) return schema;
        SchemaBuilder builder = SchemaBuilder.struct();
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            String newName = field.name().replace("-", "_");
            Schema fieldSchema = field.schema();
            if (fieldSchema.type() == Schema.Type.STRUCT) {
                fieldSchema = transformSchema(fieldSchema);
            }
            builder.field(newName, fieldSchema);
        }
        return builder.build();
    }

    private Struct processStruct(Struct struct, Schema schema) {
        Schema newSchema = transformSchema(schema);
        Struct newStruct = new Struct(newSchema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            Object value = struct.get(field);
            String newName = field.name().replace("-", "_");
            if (value instanceof Struct) {
                value = processStruct((Struct) value, field.schema());
            } else if (value instanceof Map) {
                value = processMap((Map<String, Object>) value);
            } else if (value instanceof Iterable) {
                value = processIterable((Iterable<?>) value);
            }
            newStruct.put(newName, value);
        }
        return newStruct;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public String toString() {
        return "HyphenToUnderscore{}";
    }
}
