package Recommendation.System;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CityBlockSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;

public class Main {

    public static class Ratings {
        @RowKey
        String userid;
        String item;
        String rating;
    }

    public static void main(String[] args) throws IOException, GSException {

        // **GridDB Connection**
        Properties props = new Properties();
        props.setProperty("notificationAddress", "239.0.0.1");
        props.setProperty("notificationPort", "31999");
        props.setProperty("clusterName", "defaultCluster");
        props.setProperty("user", "admin");
        props.setProperty("password", "admin");

        GridStore store = GridStoreFactory.getInstance().getGridStore(props);
        Collection<String, Ratings> coll = store.putCollection("col01", Ratings.class);

        // **Write Data to GridDB**
        File file1 = new File("data.csv");
        Scanner sc = new Scanner(file1);
        String data = sc.next();

        while (sc.hasNext()) {
            String scData = sc.next();
            String[] dataList = scData.split(",");
            String userid = dataList;
            String item = dataList[1];
            String rating = dataList[2];

            Ratings ratings = new Ratings();
            ratings.userid = userid;
            ratings.item = item;
            ratings.rating = rating;
            coll.append(ratings);
        }

        sc.close();
        store.commit();
        store.close();

        // **Pull Data from GridDB**
        store = GridStoreFactory.getInstance().getGridStore(props);
        coll = store.getCollection("col01", Ratings.class);
        Query<Ratings> query = coll.query("select *");
        RowSet<Ratings> rs = query.fetch(false);

        // **Build Recommender System**
        try {
            CityBlockSimilarity similarity = new CityBlockSimilarity(rs);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, rs);
            UserBasedRecommender recommender = new GenericUserBasedRecommender(rs, neighborhood, similarity);

            // UserID and number of items to be recommended
            List<RecommendedItem> recommended_items = recommender.recommend(2, 2);

            for (RecommendedItem r : recommended_items) {
                System.out.println(r);
            }

        } catch (Exception ex) {
            System.out.println("An exception occurred!");
        }
    }
}