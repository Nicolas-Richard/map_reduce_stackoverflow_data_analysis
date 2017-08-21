package richard.nicolas.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MRDPUtils;

import java.io.IOException;
import java.util.Map;


public class PostMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private static Context context;
    public static final String COUNTER_GROUP_NAME = "Counters_Posts";
    private final static IntWritable one = new IntWritable(1);
    private Text link = new Text();

    // setup method is called by every mapper before starting, load the known URLs into a set
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        PostMapper.context = context;
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Parse the input string into a nice map
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                .toString());

        context.getCounter(COUNTER_GROUP_NAME, "All").increment(1);

        // Grab the necessary XML attributes
        String txt = parsed.get("Body");
        String strDate = parsed.get("CreationDate");

        if (txt == null) {
            return;
        }

        String url = getWikipediaURLFromPost(txt);
        if (url == null) {
            return;
        }

        System.out.println("[+] Sent post record to reducer :\t" + strDate);
        link.set(url);
        context.write(link, one);

    }

    public static String getWikipediaURLFromPost(String text) {

        String delimiter = "&quot;";
        String result;

        int idx = text.indexOf("&quot;http://en.wikipedia.org");
        if (idx == -1) {
            return null;
        } else {
            context.getCounter(COUNTER_GROUP_NAME, "Wikipedia_URL_Found").increment(1);
        }

        int idx_end = text.indexOf("&quot;", idx + 1);

        if (idx_end == -1) {
            return null;
        }

        int idx_hash = text.indexOf('#', idx + 1);

        if (idx_hash != -1 && idx_hash < idx_end) {
            result = text.substring(idx + delimiter.length(), idx_hash);
        } else {
            result = text.substring(idx + delimiter.length(), idx_end);
        }

        return result;

    }

}

