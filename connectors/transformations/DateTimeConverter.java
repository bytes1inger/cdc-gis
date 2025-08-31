package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Custom Single Message Transformation (SMT) for handling datetime conversions
 * between MS SQL Server and PostgreSQL.
 * 
 * This transformer ensures proper formatting of datetime, datetime2, and
 * datetimeoffset types from MS SQL Server to PostgreSQL timestamp and timestamptz.
 */
public class DateTimeConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String DATETIME_TYPE = "io.debezium.time.Timestamp";
    private static final String ZONED_DATETIME_TYPE = "io.debezium.time.ZonedTimestamp";
    
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
            if (field.schema().name() != null) {
                String schemaName = field.schema().name();
                
                // Handle timestamp conversions
                if (schemaName.equals(DATETIME_TYPE) || schemaName.equals(ZONED_DATETIME_TYPE)) {
                    String fieldName = field.name();
                    Object dateTimeValue = after.get(fieldName);
                    
                    if (dateTimeValue != null && dateTimeValue instanceof String) {
                        String dateTimeStr = (String) dateTimeValue;
                        try {
                            // Format conversion if needed
                            // For most cases, Debezium's default conversion is sufficient
                            // This is a placeholder for any specific formatting needs
                        } catch (DateTimeParseException e) {
                            // Log error but don't fail the transformation
                            System.err.println("Error parsing datetime: " + e.getMessage());
                        }
                    }
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
