package edu.umkc.cs5540;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/query")
public class QueryController
{
    private SparkConf configuration = new SparkConf().setAppName("TwitterSpark").setMaster("local[*]");
    private JavaSparkContext context = new JavaSparkContext(configuration);
    private SparkSession session = SparkSession.builder().appName("TwitterSpark").config(configuration).getOrCreate();

    @CrossOrigin
    @RequestMapping(path="/usage", method=RequestMethod.GET)
    public @ResponseBody List<TimeSeries> getUsageRatio() {
        Dataset<Row> df = session.read().json("data/input/tweets-test.json");
        df.createOrReplaceTempView("tweets");
        df = session.sql("SELECT explode(entities.hashtags) AS hashtag, user FROM tweets");
        df.createOrReplaceTempView("exploded_view");
        df = session.sql("SELECT hashtag.text, COUNT(hashtag), COUNT(DISTINCT user.id) FROM exploded_view GROUP BY hashtag.text ORDER BY COUNT(user.id) DESC LIMIT 5");
        df.show(50, false);
        List<Row> ds = df.collectAsList();
        return ds.stream().map(row -> new TimeSeries(row.getString(0), row.getLong(1) - row.getLong(2), row.getLong(2))).collect(Collectors.toList());
    }

    @CrossOrigin
    @RequestMapping(path="/influence", method=RequestMethod.GET)
    public @ResponseBody List<Tuple2<String, Long>> getUserInfluence() {
        JavaRDD<Status> initial = context.textFile("data/input/tweets-test.json").map(TwitterObjectFactory::createStatus);
        JavaPairRDD<User, Iterable<Status>> statusByUsers = initial.groupBy(Status::getUser);
        JavaPairRDD<String, Long> scoredUsers = statusByUsers.mapToPair(sbu -> {
            int tweets = 0, retweets = 0;
            long score = sbu._1().getFollowersCount() + sbu._1().getFriendsCount() + sbu._1().getListedCount();
            for (Status status : sbu._2()) {
                tweets++;
                if (status.isRetweet()) {
                    retweets++;
                }
            }
            double ratio = retweets > 0 ? tweets / retweets : 1;
            return new Tuple2<>(sbu._1().getScreenName(), Math.round(score * ratio));
        });
        return scoredUsers.map(a -> a).sortBy(Tuple2::_2, false, 1).cache().take(5);
    }

    @CrossOrigin
    @RequestMapping(path="/trends", method=RequestMethod.GET)
    public @ResponseBody List<TimeSeries> getTrendsOverTime(@RequestParam(value="name", required=false) String name) {
        Dataset<Row> df = session.read().json("data/input/trends.txt");
        df.createOrReplaceTempView("twitter_trends");
        df = session.sql("SELECT details.as_of, explode(details.trends) AS trends FROM twitter_trends");
        df.createOrReplaceTempView("trends");
        df = session.sql("SELECT as_of, explode(trends) AS trend FROM trends");
        df.createOrReplaceTempView("trend");
        df = session.sql("SELECT trend.name, explode(as_of) AS as_of, trend.tweet_volume FROM trend");
        df.createOrReplaceTempView("temp");
        df = session.sql("SELECT name, collect_list(as_of) AS timestamps, collect_list(tweet_volume) AS tweets FROM temp WHERE tweet_volume IS NOT NULL GROUP BY name ORDER BY tweets DESC LIMIT 1");
        df.show(50, false);
        return df.collectAsList().stream().map(row -> new TimeSeries(row.getString(0), row.getList(1), row.getList(2))).collect(Collectors.toList());
    }

    @CrossOrigin
    @RequestMapping(path="/popular", method=RequestMethod.GET)
    public @ResponseBody List<Tuple2<String, Long>> getPopularHashtags(@RequestParam(value="count", required=false, defaultValue="10") Integer count) {
        return getTopHashtags(count);
    }

    private List<Tuple2<String, Long>> getTopHashtags(int count) {
        JavaRDD<String> tweets = context.textFile("data/input/tweets-test.json");
        JavaPairRDD<String, Long> hashtags = tweets.map(tweet -> TwitterObjectFactory.createStatus(tweet).getHashtagEntities())
                .flatMap(hashtagHolder -> Arrays.asList(hashtagHolder).iterator())
                .mapToPair(hashtag -> new Tuple2<>(hashtag.getText(), 1l))
                .reduceByKey((a, b) -> a + b);
        return hashtags.map(a -> a).sortBy(Tuple2::_2, false, 1).cache().take(count);
    }

    private String getTopHashtag() {
        return getPopularHashtags(1).get(0)._1();
    }
}