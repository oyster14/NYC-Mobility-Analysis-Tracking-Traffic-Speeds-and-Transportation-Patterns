import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.WKTReader;

public class TaxiZonesJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private STRtree strTree;
    private GeometryFactory geometryFactory;
    private WKTReader reader;
    private HashMap<MultiPolygon, Integer> polygonToId;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        strTree = new STRtree(); // build spatial index tree
        geometryFactory = new GeometryFactory();
        reader = new WKTReader(geometryFactory); // deserialize multi-polygon
        polygonToId = new HashMap<MultiPolygon, Integer>();

        URI[] files = context.getCacheFiles(); // get small table

        for (URI file : files) {
            if (!file.toString().endsWith(".txt")) {
                continue;
            }
            Path path = new Path(file.getPath());
            BufferedReader bf = new BufferedReader(new FileReader(path.getName())); // use local io to read small table
            String line = null;
            while ((line = bf.readLine()) != null) {
                String[] parts = line.split("@");
                String wktPolygon = parts[0];
                int id = Integer.parseInt(parts[1]);
                MultiPolygon polygon;
                try {
                    polygon = (MultiPolygon) reader.read(wktPolygon);
                } catch (org.locationtech.jts.io.ParseException e) {
                    throw new RuntimeException("Parsing polygon error, stop!", e);
                }
                strTree.insert(polygon.getEnvelopeInternal(), polygon); // create a slightly big box for polygon
                polygonToId.put(polygon, id);
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        double lat = Double.parseDouble(parts[2]);
        double lon = Double.parseDouble(parts[3]);
        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
        // create a small box for the point and get possible polygon boxes intersected
        // with the point box
        List<MultiPolygon> queryResults = strTree.query(point.getEnvelopeInternal());
        for (MultiPolygon polygon : queryResults) {
            if (polygon.contains(point)) { // find the exact polygon that contains point, and do inner join
                StringBuilder sb = new StringBuilder();
                sb.append(parts[0]);
                sb.append(",");
                sb.append(parts[1]);
                sb.append(",");
                sb.append(Integer.toString(polygonToId.get(polygon)));
                context.write(NullWritable.get(), new Text(sb.toString()));
                break;
            }
        }
    }

    // @Override
    // public void cleanup(Context context) throws IOException, InterruptedException
    // {
    // context.write(NullWritable.get(), new
    // Text(Integer.toString(polygonToId.size())));
    // }
}
