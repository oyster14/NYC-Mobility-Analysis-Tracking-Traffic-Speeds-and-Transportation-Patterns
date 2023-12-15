package org.cleaner.cleaner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import java.io.IOException;

public class cleanMapper extends Mapper<LongWritable, Group, Void, Group> {
    private SimpleGroupFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String writeSchema = "message trip_record { " +
                "required binary pickup_datetime (UTF8);" +
                "required binary dropOff_datetime (UTF8);" +
                "required int64 PULocationID;" +
                "required int64 DOLocationID;" +
                "}";
        MessageType schema = MessageTypeParser.parseMessageType(writeSchema);
        factory = new SimpleGroupFactory(schema);
    }

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        if (isValidRecord(value)) {
            Group newRecord = factory.newGroup()
                    .append("pickup_datetime", value.getString("pickup_datetime", 0))
                    .append("dropOff_datetime", value.getString("dropOff_datetime", 0))
                    .append("PULocationID", value.getLong("PULocationID", 0))
                    .append("DOLocationID", value.getLong("DOLocationID", 0));

            context.write(null, newRecord);
        }
    }

    private boolean isValidRecord(Group value) {
        GroupType schema = value.getType();

        return isFieldValid(value, schema, "pickup_datetime") &&
                isFieldValid(value, schema, "dropOff_datetime") &&
                isFieldValid(value, schema, "PULocationID") &&
                isFieldValid(value, schema, "DOLocationID");
    }

    private boolean isFieldValid(Group group, GroupType schema, String fieldName) {
        if (!schema.containsField(fieldName) || group.getFieldRepetitionCount(fieldName) == 0) {
            return false;
        }

        if (fieldName.equals("PULocationID") || fieldName.equals("DOLocationID")) {
            long value = group.getLong(fieldName, 0);
            return value != 0;
        }

        String value = group.getString(fieldName, 0);
        return value != null && !value.isEmpty();
    }
}