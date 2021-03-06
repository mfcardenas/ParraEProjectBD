import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class WEParraDataTwitter {

    private static Logger logger = LoggerFactory.getLogger(WEParraDataTwitter.class);
    private static Utils obj = new Utils();

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final Properties properties = new Properties();

        properties.setProperty(TwitterSource.CONSUMER_KEY,obj.getKey("twitter.consumerKey"));
        properties.setProperty(TwitterSource.CONSUMER_SECRET,obj.getKey("twitter.consumerSecret"));
        properties.setProperty(TwitterSource.TOKEN,obj.getKey("twitter.token"));
        properties.setProperty(TwitterSource.TOKEN_SECRET,obj.getKey("twitter.secret"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        final DataStream<String> streamSource = env.addSource(new TwitterSource(properties));
        DataStream<Tuple5<String, Integer, Double, Double, Integer>> dataStream =
                streamSource.flatMap(new HashtagTokenizeFlatMap()).keyBy(0).sum(4);
        dataStream.print();

        Map<String, String> config = new HashMap<>();
        // Esto instruye al receptor para que emita después de cada elemento, de lo contrario, se almacenarían en el búfer
        config.put("bulk.flush.max.actions", "10");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        dataStream.addSink(new ElasticsearchSink<>(config, transports, new TwitterInserter()));

        env.execute("Flujo Tweets WEParra");
    }

    public static class HashtagTokenizeFlatMap
            implements FlatMapFunction<String, Tuple5<String, Integer, Double, Double, Integer>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;

        /**
         * Parsear el fichero JSON para analizar su información y procesar los campos correspondientes.
         */
        @Override
        public void flatMap(String value, Collector<Tuple5<String, Integer, Double, Double, Integer>> out)
                throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            if (value.contains("created_at")) {//filter delete record tweet
                final boolean hasHashtags = jsonNode.get("entities").get("hashtags").size() > 0;
                //https://dev.twitter.com/overview/api/tweets#obj-coordinates
                final boolean hasGeoCoordinates = jsonNode.get("geo").has("coordinates");
                final boolean hasCoordinatesCoordinates =
                        !jsonNode.get("coordinates").isNull() && jsonNode.get("coordinates").get("coordinates").size() > 0;
                if (hasHashtags && (hasGeoCoordinates || hasCoordinatesCoordinates)) {
                    final double latitude = hasGeoCoordinates ? jsonNode.get("geo").get("coordinates").get(0).asDouble() :
                            jsonNode.get("coordinates").get("coordinates").get(1).asDouble();
                    final double longitude = hasGeoCoordinates ? jsonNode.get("geo").get("coordinates").get(1).asDouble() :
                            jsonNode.get("coordinates").get("coordinates").get(0).asDouble();
                    for (int i = 0; i < jsonNode.get("entities").get("hashtags").size(); i++) {
                        StringTokenizer tokenizer =
                                new StringTokenizer(jsonNode.get("entities").get("hashtags").get(i).get("text").asText());
                        while (tokenizer.hasMoreTokens()) {
                            String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
                            int followersCount = 0;
                            if (jsonNode.get("user").has("followers_count")) {
                                followersCount = jsonNode.get("user").get("followers_count").asInt(0);
                            }
                            if (!result.equals("")) {
                                out.collect(new Tuple5<>(result, followersCount, latitude, longitude, 1));
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Crear los indices.
     */
    public static class TwitterInserter
            implements ElasticsearchSinkFunction<Tuple5<String, Integer, Double, Double, Integer>> {

        /**
         *
         * @param record a
         * @param ctx b
         * @param indexer c
         */
        @Override
        public void process(Tuple5<String, Integer, Double, Double, Integer> record, RuntimeContext ctx,
                            RequestIndexer indexer) {
            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("hashtag", record.f0);                                         // hashtag
            json.put("followers_count", record.f1.toString());                      // followers count
            json.put("location", record.f2 + "," + record.f3);                      // lat,lon pair
            json.put("count", record.f4.toString());                                //count of the hashtag
            IndexRequest rqst = Requests.indexRequest().index("parra-twits")        // index name
                    .type("twitter-location")                                           // mapping name
                    .source(json);

            System.out.println(json.toString());
            indexer.add(rqst);
        }
    }
}
