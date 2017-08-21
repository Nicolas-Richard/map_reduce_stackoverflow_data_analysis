package richard.nicolas.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MRDPUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CommentMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private static Context context;
    public static final String COUNTER_GROUP_NAME = "Counters_Comments";
    private static Set<String> knownValidURLs = new HashSet<String>();
    private final static IntWritable one = new IntWritable(1);
    private Text link = new Text();

    // setup method is called by every mapper before starting, load the known URLs into a set
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        CommentMapper.context = context;
        URI[] files = context.getCacheFiles();

        // if the files in the distributed cache are set
        if (files != null && files.length == 1) {
            System.out.println("Reading cached URLs from: " + files[0].getPath());

            try {
                File f = new File(files[0].getPath());
                BufferedReader b = new BufferedReader(new FileReader(f));
                String readLine;
                System.out.println("Reading cached URLs in Buffered Reader");
                while ((readLine = b.readLine()) != null) {
                    //System.out.println(readLine);
                    knownValidURLs.add(readLine);
                }

                System.out.println("Length of the set created of the Cached URLs: " + knownValidURLs.size());

            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            throw new IOException(
                    "Cache file not set in the DistributedCache.");
        }
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Parse the input string into a nice map
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                .toString());

        context.getCounter(COUNTER_GROUP_NAME, "All").increment(1);

        // Grab the necessary XML attributes
        String txt = parsed.get("Text");
        String strDate = parsed.get("CreationDate");

        if (txt == null) {
            return;
        }

        String url = getWikipediaURL(txt);
        if (url == null) {
            return;
        }

        System.out.println("[+] Sent comment record to reducer :\t" + strDate);

        link.set(url);
        context.write(link, one);
    }

    public static String getWikipediaURL(String text) {

        int idx = text.indexOf("http://en.wikipedia.org/wiki/");
        if (idx == -1) {
            return null;
        } else {
            // A wikipedia URL has been found. Increment the counter
            context.getCounter(COUNTER_GROUP_NAME, "Wikipedia_URL_Found").increment(1);
        }

        String result = text.substring(idx, text.length());

        // Remove anything after an HTML anchor
        int idx_hash = text.indexOf('#', idx + 1);
        if (idx_hash != -1) {
            result = text.substring(idx, idx_hash);
        }

        // Remove anything after a whitespace
        result = result.split("\\s+")[0];

        // Lots of URLs in the data are 'http' but now Wikipedia always redirects to 'https'
        result = result.replaceAll("^http", "https");

        // Lots of URLs can be validated after removing a trailing character or 2
        ArrayList<String> URLsToTest = new ArrayList<String>();
        URLsToTest.add(result);
        URLsToTest.add(result.substring(0, result.length() - 1));
        URLsToTest.add(result.substring(0, result.length() - 2));

        System.out.println("[-] Processing :\t" + result);

        // Test if any version of this URL is in the cache
        for (String URLToTest : URLsToTest) {
            if (checkURLIsInCache(URLToTest)) {
                return URLToTest;
            }
        }

        // Test if any version of this URL is reachable
        for (String URLToTest : URLsToTest) {
            if (checkURLIsReachable(URLToTest)) {
                return URLToTest;
            }
        }
        System.out.println("[x] URL processing failed :\t" + result);
        return null;
    }

    public static Boolean checkURLIsInCache(String text) {
        if (knownValidURLs.contains(text)) {
            System.out.println("[+] Fnd in Cache :\t" + text);
            context.getCounter(COUNTER_GROUP_NAME, "Cache_Hits").increment(1);
            return true;
        }
        return false;
    }

    public static Boolean checkURLIsReachable(String result) {
        try {
            URL url = new URL(result);
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
            Integer statusCode = http.getResponseCode();
            context.getCounter(COUNTER_GROUP_NAME, statusCode.toString()).increment(1);
            // System.out.println(statusCode);
            if (statusCode.equals(200)) {
                System.out.println("[+] URL reached :\t" + result);
                return true;
            }
        } catch (MalformedURLException e) {
            // the URL is not in a valid form
        } catch (IOException e) {
            // the connection couldn't be established
        }
        return false;
    }


}
