package grupoexito.poc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Main.class);
        logger.info("Comenzando pipeline");

        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline p = Pipeline.create(options);
        p.apply("ReadMongoDB", MongoDbIO.read()
                .withUri("mongodb+srv://test-gcp:PBkmzLF99iVElBa3@cluster0.ok0ro.mongodb.net")
                .withDatabase("cafe")
                .withCollection("usuarios")
                .withBucketAuto(true)
        );

        p.run();

    }
}
