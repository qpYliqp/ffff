package crtracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import scala.Tuple2;

public class CRTools {
    public static final int WEEKS = 9;
    private static final Gson GSON = new Gson();
    private static final Instant COLLECT_START = Instant.parse("2024-09-26T09:00:00Z");

    public static JavaRDD<Battle> getDistinctRawBattles(JavaSparkContext sc, int weeks) {

        Instant sliding_window = Instant.now().minusSeconds(3600L * 24 * 7 * weeks);
        JavaRDD<Battle> rddpair = sc.textFile("../auber/data_ple/clashroyale2024/clash_big.nljson")
        .flatMapToPair(line -> {
            List<Tuple2<String, Battle>> results = new ArrayList<>();
            
            if (!line.isEmpty()) {
                Battle battle = GSON.fromJson(line, Battle.class);
             
                if (battle.players.get(0).deck.length() != 16 || 
                battle.players.get(1).deck.length() != 16 ||
                Math.abs(battle.players.get(0).strength - battle.players.get(1).strength) > 0.75 ||
                battle.players.get(0).touch != 1 || 
                battle.players.get(1).touch != 1 ||
                battle.players.get(0).bestleague < 6 || 
                battle.players.get(1).bestleague < 6) 
                {
                    return results.iterator();
                }
    
                    battle.setParsedDate();
                    String u1 = battle.players.get(0).utag;
                    String u2 = battle.players.get(1).utag;
                    String key = battle.date + "_" + battle.round + "_" + (u1.compareTo(u2) < 0 ? u1 + u2 : u2 + u1);
                    results.add(new Tuple2<>(key, battle));
                
            }
            return results.iterator();
            })
            .distinct().values()
            .filter((Battle battle) -> { 
                Instant inst = Instant.parse(battle.date);
                return inst.isAfter(sliding_window) && inst.isAfter(COLLECT_START);}
            );
    

        
        

		rddpair = rddpair.filter((Battle x) -> {
			Instant inst = Instant.parse(x.date);
			return inst.isAfter(sliding_window) && inst.isAfter(COLLECT_START);
    	});

        JavaPairRDD<String, Iterable<Battle>> rddbattles = rddpair.mapToPair((d) -> {
            String u1 = d.players.get(0).utag;
            String u2 = d.players.get(1).utag;
            double e1 = d.players.get(0).elixir;
            double e2 = d.players.get(1).elixir;
            return new Tuple2<>(d.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + e1 + u2 + e2 : u2 + e2 + u1 + e1), d);
        }).groupByKey();

        JavaRDD<Battle> clean = rddbattles.values().flatMap((it) -> {
            ArrayList<Battle> lbattles = new ArrayList<>();
            ArrayList<Battle> rbattles = new ArrayList<>();
            for (Battle bi : it)
                lbattles.add(bi);
            lbattles.sort((Battle x, Battle y) -> {
                if (Instant.parse(x.date).isAfter(Instant.parse(y.date)))
                    return 1;
                if (Instant.parse(y.date).isAfter(Instant.parse(x.date)))
                    return -1;
                return 0;
            });
            rbattles.add(lbattles.get(0));
            for (int i = 1; i < lbattles.size(); ++i) {
                long i1 = Instant.parse(lbattles.get(i - 1).date).getEpochSecond();
                long i2 = Instant.parse(lbattles.get(i).date).getEpochSecond();
                if (Math.abs(i1 - i2) > 10)
                    rbattles.add(lbattles.get(i));
            }
            return rbattles.iterator();
        });
       
        return clean;
    }
}
