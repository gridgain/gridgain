package org.gridgain.dto;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Custom ignite uuid serializer.
 */
public class IgniteUuidSerializer extends JsonSerializer<IgniteUuid> {
    /** {@inheritDoc} */
    @Override public void serialize(IgniteUuid val, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeString(val.toString());
    }
}
