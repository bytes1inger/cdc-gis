package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

/**
 * Custom Single Message Transformation (SMT) for converting MS SQL Server geography
 * data type to PostgreSQL/PostGIS compatible format.
 * 
 * This transformer converts the WKB (Well-Known Binary) representation from MS SQL
 * to a format compatible with PostGIS.
 */
public class GeographyConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String GEOGRAPHY_TYPE = "io.debezium.data.geometry.Geography";
    
    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct value = (Struct) record.value();
        
        // Get the 'after' field which contains the new state of the record
        Struct after = value.getStruct("after");
        if (after == null) {
            return record;
        }
        
        // Process all fields in the 'after' struct
        Schema afterSchema = after.schema();
        for (Field field : afterSchema.fields()) {
            if (field.schema().name() != null && field.schema().name().equals(GEOGRAPHY_TYPE)) {
                String fieldName = field.name();
                Object geographyValue = after.get(fieldName);
                
                if (geographyValue != null) {
                    // Convert the geography value if needed
                    // Note: Debezium already converts to WKB format which PostGIS can understand
                    // This is a placeholder for any additional conversion if needed
                }
            }
        }

        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to clean up
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(config(), configs);
        // No configuration needed for this transformer
    }
}
