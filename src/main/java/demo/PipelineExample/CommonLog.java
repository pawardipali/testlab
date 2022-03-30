package demo.PipelineExample;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * A class used for parsing JSON web server events
 * Annotated with @DefaultSchema to the allow the use of Beam Schema and <Row> object
 */
@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    int id;
    String name;
    String surname;
}
