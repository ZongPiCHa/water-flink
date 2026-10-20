package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.*;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import com.zongpicha.waterflink.model.EngagementAccumulator;
import com.zongpicha.waterflink.model.HeatScoreRule;

class StateEvolutionTest {
    /** Run with -Dprevious.jar=/path/to/the/actually/deployed.jar before upgrade. */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void restoresPreviousAccumulatorAndBroadcastRule() throws Exception {
        String previousJar = System.getProperty("previous.jar");
        Assumptions.assumeTrue(previousJar != null, "provide previous.jar for release validation");
        ClassLoader currentLoader = getClass().getClassLoader();
        try (URLClassLoader oldLoader = new URLClassLoader(
                new URL[]{Path.of(previousJar).toUri().toURL()}, currentLoader) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("com.zongpicha.waterflink.model.")) {
                    Class<?> type = findLoadedClass(name);
                    if (type == null) type = findClass(name);
                    if (resolve) resolveClass(type);
                    return type;
                }
                return super.loadClass(name, resolve);
            }
        }) {
            for (Class<?> currentType : new Class<?>[]{
                    EngagementAccumulator.class, HeatScoreRule.class}) {
                Class<?> oldType = oldLoader.loadClass(currentType.getName());
                Object value = oldType.getConstructor().newInstance();
                String setter = currentType == EngagementAccumulator.class ? "setLikes" : "setVersion";
                oldType.getMethod(setter, long.class).invoke(value, 37L);
                TypeSerializer oldSerializer = TypeInformation.of(oldType)
                        .createSerializer(new SerializerConfigImpl());
                TypeSerializer newSerializer = TypeInformation.of(currentType)
                        .createSerializer(new SerializerConfigImpl());
                assertInstanceOf(PojoSerializer.class, oldSerializer);
                assertInstanceOf(PojoSerializer.class, newSerializer);
                DataOutputSerializer data = new DataOutputSerializer(256);
                oldSerializer.serialize(value, data);
                DataOutputSerializer metadata = new DataOutputSerializer(256);
                TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                        metadata, oldSerializer.snapshotConfiguration());
                TypeSerializerSnapshot oldSnapshot =
                        TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                                new DataInputDeserializer(metadata.getCopyOfBuffer()), currentLoader);
                TypeSerializerSchemaCompatibility compatibility =
                        newSerializer.snapshotConfiguration().resolveSchemaCompatibility(oldSnapshot);
                assertFalse(compatibility.isIncompatible(), currentType.getName());
                // Migration reads bytes using the restored old schema under the new classloader.
                Object restored = oldSnapshot.restoreSerializer()
                        .deserialize(new DataInputDeserializer(data.getCopyOfBuffer()));
                if (restored instanceof EngagementAccumulator accumulator) {
                    assertEquals(37L, accumulator.getLikes());
                    assertNull(accumulator.getExtraMetrics());
                } else {
                    assertEquals(37L, ((HeatScoreRule) restored).getVersion());
                    assertNull(((HeatScoreRule) restored).getWeights());
                }
            }
        }
    }
}
