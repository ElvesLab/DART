import com.mongodb.DB;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SaveStateToDB {

    public SaveStateToDB(String stateFile) throws IOException {
        // mongodb://elves:Elves123@ds147451.mlab.com:47451/stormstate
        // Creating a Mongo client
        MongoClientURI uri = new MongoClientURI("mongodb://elves:Elves123@ds147451.mlab.com:47451/stormstate?retryWrites=false");
        MongoClient mongo = new MongoClient(uri);

        // Accessing the database
        MongoDatabase database = mongo.getDatabase("stormstate");

        // Retrieving a collection
        MongoCollection<Document> collection = database.getCollection("stateCollection");
        System.out.println("Collection sampleCollection selected successfully");

        final String s = stateFile;
        byte[] contentByteArray = Files.readAllBytes(Paths.get(s));

        Document document = new Document("title", "stormState")
                .append("state", contentByteArray);
        collection.insertOne(document);
        System.out.println("State document inserted into DB successfully.");

    }
}
