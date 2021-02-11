package grupoexito.poc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Main.class);
        logger.info("Comenzando pipeline");
        runPoc();
        logger.info("Finalizando pipeline");
    }

    private static void runPoc() {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);
        PCollection<Document> mongo = p.apply("ReadMongoDB", MongoDbIO.read()
                .withUri("mongodb+srv://test-gcp:PBkmzLF99iVElBa3@cluster0.ok0ro.mongodb.net")
                .withDatabase("cafe")
                .withCollection("usuarios")
                .withBucketAuto(true)
        );

        p.run().waitUntilFinish();
    }
}
