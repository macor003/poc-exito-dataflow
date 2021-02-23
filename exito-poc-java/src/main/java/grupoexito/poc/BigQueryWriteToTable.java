package grupoexito.poc;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.values.PCollection;

class BigQueryWriteToTable {
    public static void writeToTable(
            String project,
            String dataset,
            String table,
            TableSchema schema,
            PCollection<TableRow> rows) {

        // String project = "my-project-id";
        // String dataset = "my_bigquery_dataset_id";
        // String table = "my_bigquery_table_id";

        // TableSchema schema = new TableSchema().setFields(Arrays.asList(...));

        // Pipeline pipeline = Pipeline.create();
        // PCollection<TableRow> rows = ...

        rows.apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.%s", project, dataset, table))
                        .withSchema(schema)
                        // For CreateDisposition:
                        // - CREATE_IF_NEEDED (default): creates the table if it doesn't exist, a schema is
                        // required
                        // - CREATE_NEVER: raises an error if the table doesn't exist, a schema is not needed
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        // For WriteDisposition:
                        // - WRITE_EMPTY (default): raises an error if the table is not empty
                        // - WRITE_APPEND: appends new rows to existing rows
                        // - WRITE_TRUNCATE: deletes the existing rows before writing
                        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));

        // pipeline.run().waitUntilFinish();
    }
}
