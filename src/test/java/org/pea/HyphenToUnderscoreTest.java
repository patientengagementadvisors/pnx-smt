package org.pea;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HyphenToUnderscoreTest {
    @Test
    void testMapWithHyphens() {
        Map<String, Object> value = new HashMap<>();
        value.put("foo-bar", 123);
        value.put("baz-qux", "abc");
        SourceRecord record = new SourceRecord(null, null, "topic", null, null, null, null, value, null);
        HyphenToUnderscore transform = new HyphenToUnderscore();
        SourceRecord result = transform.apply(record);
        System.out.println("testMapWithHyphens: result.value() = " + result.value());
        Map<?, ?> newValue = (Map<?, ?>) result.value();
        assertTrue(newValue.containsKey("foo_bar"));
        assertTrue(newValue.containsKey("baz_qux"));
        assertFalse(newValue.containsKey("foo-bar"));
    }

    @Test
    void testStructWithHyphens() {
        Schema schema = SchemaBuilder.struct().field("foo-bar", Schema.INT32_SCHEMA).field("baz-qux", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("foo-bar", 42);
        struct.put("baz-qux", "hello");
        SourceRecord record = new SourceRecord(null, null, "topic", null, null, null, schema, struct, null);
        HyphenToUnderscore transform = new HyphenToUnderscore();
        SourceRecord result = transform.apply(record);
        System.out.println("testStructWithHyphens: result.value() = " + result.value());
        Struct newStruct = (Struct) result.value();
        assertEquals(42, newStruct.get("foo_bar"));
        assertEquals("hello", newStruct.get("baz_qux"));
    }

    @Test
    void testMapNested() {
        Map<String, Object> inner = new HashMap<>();
        inner.put("bar-baz", 456);
        Map<String, Object> value = new HashMap<>();
        value.put("foo-bar", inner);
        SourceRecord record = new SourceRecord(null, null, "topic", null, null, null, null, value, null);
        HyphenToUnderscore transform = new HyphenToUnderscore();
        SourceRecord result = transform.apply(record);
        System.out.println("testMapNested: result.value() = " + result.value());
        Map<?, ?> newValue = (Map<?, ?>) result.value();
        Map<?, ?> nested = (Map<?, ?>) newValue.get("foo_bar");
        assertTrue(nested.containsKey("bar_baz"));
    }

    @Test
    void testMapDeeplyNestedHyphens() {
        Map<String, Object> deep = new HashMap<>();
        deep.put("deep-key", 1);
        Map<String, Object> mid = new HashMap<>();
        mid.put("baz-qux", deep);
        Map<String, Object> value = new HashMap<>();
        value.put("foo-bar", mid);
        value.put("outer-key", 2);
        SourceRecord record = new SourceRecord(null, null, "topic", null, null, null, null, value, null);
        HyphenToUnderscore transform = new HyphenToUnderscore();
        SourceRecord result = transform.apply(record);
        System.out.println("testMapDeeplyNestedHyphens: result.value() = " + result.value());
        Map<?, ?> newValue = (Map<?, ?>) result.value();
        assertTrue(newValue.containsKey("foo_bar"));
        assertTrue(newValue.containsKey("outer_key"));
        assertFalse(newValue.containsKey("foo-bar"));
        assertFalse(newValue.containsKey("outer-key"));
        Map<?, ?> midLevel = (Map<?, ?>) newValue.get("foo_bar");
        assertTrue(midLevel.containsKey("baz_qux"));
        assertFalse(midLevel.containsKey("baz-qux"));
        Map<?, ?> deepLevel = (Map<?, ?>) midLevel.get("baz_qux");
        assertTrue(deepLevel.containsKey("deep_key"));
        assertFalse(deepLevel.containsKey("deep-key"));
        assertEquals(1, deepLevel.get("deep_key"));
        assertEquals(2, newValue.get("outer_key"));
    }
}
