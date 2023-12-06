import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

public class NYCTaxiDataCleanMapper extends Mapper<LongWritable, Group, Void, Group> {
    private static final Log LOG = new Log(NYCTaxiDataCleanMapper.class);
    private GroupFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Create the GroupFactory using the schema set in the main method
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
    }

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        if (isValidRecord(value)) {
            context.write(null, parseRecord(value));
        }
    }

    private boolean isValidRecord(Group value) {
        long passenger_count = 0;
        double trip_distance = 0.0;
        double total_amount = 0.0;

        GroupType inputType = value.getType();
        if (value.getFieldRepetitionCount("passenger_count") > 0) {
            // Check if the field is a Long
            if (inputType.getType("passenger_count").isPrimitive() &&
                    inputType.getType("passenger_count").asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
                passenger_count = value.getLong("passenger_count", 0);
            } else {
                // Otherwise, it's a Double
                passenger_count = (long) value.getDouble("passenger_count", 0);
            }
        }
        if (value.getFieldRepetitionCount("trip_distance") > 0) {
            trip_distance = value.getDouble("trip_distance", 0);
        }
        if (value.getFieldRepetitionCount("total_amount") > 0) {
            total_amount = value.getDouble("total_amount", 0);
        }

        return (passenger_count > 0 && trip_distance > 0 && total_amount > 0);
    }

    private Group parseRecord(Group value) {
        long vendor_id = 0;
        long pickup_datetime = 0;
        long dropoff_datetime = 0;
        long passenger_count = 0;
        double trip_distance = 0.0;
        long pickup_locationId = 0;
        long dropoff_locationId = 0;
        long payment_type = 0;
        double fare_amount = 0.0;
        double extra = 0.0;
        double mta_tax = 0.0;
        double tip_amount = 0.0;
        double tolls_amount = 0.0;
        double improvement_surcharge = 0.0;
        double total_amount = 0.0;
        double congestion_surcharge = 0.0;
        double airport_fee = 0.0;
        
        GroupType inputType = value.getType();

        if (value.getFieldRepetitionCount("VendorID") > 0) {
            if (inputType.getType("VendorID").isPrimitive() &&
                    inputType.getType("VendorID").asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
                vendor_id = (long) value.getInteger("VendorID", 0);
            } else {
                vendor_id = value.getLong("VendorID", 0);
            }
        }
        if (value.getFieldRepetitionCount("tpep_pickup_datetime") > 0) {
            pickup_datetime = value.getLong("tpep_pickup_datetime", 0);
        }
        if (value.getFieldRepetitionCount("tpep_dropoff_datetime") > 0) {
            dropoff_datetime = value.getLong("tpep_dropoff_datetime", 0);
        }
        if (value.getFieldRepetitionCount("passenger_count") > 0) {
            if (inputType.getType("passenger_count").isPrimitive() &&
                    inputType.getType("passenger_count").asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
                passenger_count = value.getLong("passenger_count", 0);
            } else {
                passenger_count = (long) value.getDouble("passenger_count", 0);
            }
        }
        if (value.getFieldRepetitionCount("trip_distance") > 0) {
            trip_distance = value.getDouble("trip_distance", 0);
        }
        if (value.getFieldRepetitionCount("PULocationID") > 0) {
            if (inputType.getType("PULocationID").isPrimitive() &&
                    inputType.getType("PULocationID").asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
                pickup_locationId = (long) value.getInteger("PULocationID", 0);
            } else {
                pickup_locationId = value.getLong("PULocationID", 0);
            }
        }
        if (value.getFieldRepetitionCount("DOLocationID") > 0) {
            if (inputType.getType("DOLocationID").isPrimitive() &&
                    inputType.getType("DOLocationID").asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
                dropoff_locationId = (long) value.getInteger("DOLocationID", 0);
            } else {
                dropoff_locationId = value.getLong("DOLocationID", 0);
            }
        }
        if (value.getFieldRepetitionCount("payment_type") > 0) {
            payment_type = value.getLong("payment_type", 0);
        }
        if (value.getFieldRepetitionCount("fare_amount") > 0) {
            fare_amount = value.getDouble("fare_amount", 0);
        }
        if (value.getFieldRepetitionCount("extra") > 0) {
            extra = value.getDouble("extra", 0);
        }
        if (value.getFieldRepetitionCount("mta_tax") > 0) {
            mta_tax = value.getDouble("mta_tax", 0);
        }
        if (value.getFieldRepetitionCount("tip_amount") > 0) {
            tip_amount = value.getDouble("tip_amount", 0);
        }
        if (value.getFieldRepetitionCount("tolls_amount") > 0) {
            tolls_amount = value.getDouble("tolls_amount", 0);
        }
        if (value.getFieldRepetitionCount("improvement_surcharge") > 0) {
            improvement_surcharge = value.getDouble("improvement_surcharge", 0);
        }
        if (value.getFieldRepetitionCount("total_amount") > 0) {
            total_amount = value.getDouble("total_amount", 0);
        }
        if (value.getFieldRepetitionCount("congestion_surcharge") > 0) {
            total_amount = value.getDouble("congestion_surcharge", 0);
        }
        if (inputType.containsField("airport_fee") && value.getFieldRepetitionCount("airport_fee") > 0) {
            airport_fee = value.getDouble("airport_fee", 0);
        } else if (inputType.containsField("Airport_fee") && value.getFieldRepetitionCount("Airport_fee") > 0) {
            airport_fee = value.getDouble("Airport_fee", 0);
        }

        Group record = factory.newGroup()
                .append("vendor_id", vendor_id)
                .append("pickup_datetime", pickup_datetime)
                .append("dropoff_datetime", dropoff_datetime)
                .append("passenger_count", passenger_count)
                .append("trip_distance", trip_distance)
                .append("pickup_locationId", pickup_locationId)
                .append("dropoff_locationId", dropoff_locationId)
                .append("payment_type", payment_type)
                .append("fare_amount", fare_amount)
                .append("extra", extra)
                .append("mta_tax", mta_tax)
                .append("tip_amount", tip_amount)
                .append("tolls_amount", tolls_amount)
                .append("improvement_surcharge", improvement_surcharge)
                .append("total_amount", total_amount)
                .append("congestion_surcharge", congestion_surcharge)
                .append("airport_fee", airport_fee);
        return record;
    }

}