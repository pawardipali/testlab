package demo.PipelineExample;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	/**
	 * The logger to output status messages to.
	 */
    public static final TupleTag<CommonLog> VALID_MSG = new TupleTag<CommonLog>() {
		private static final long serialVersionUID = 1L;
	};
	public static final TupleTag<String> INVALID_MSG = new TupleTag<String>() {
		private static final long serialVersionUID = 1L;
	};
    
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	public static void main(String[] args) throws Exception {
	 DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		options.setJobName("usecase1-labid-3");
		options.setRegion("europe-west4");
		options.setGcpTempLocation("gs://c4e-uc1-dataflow-temp-3/temp");
		options.setStagingLocation("gs://c4e-uc1-dataflow-temp-3/staging");
		//options.setServiceAccount("c4e-uc1-sa-3@nttdata-c4e-bde.iam.gserviceaccount.com");
		options.setWorkerMachineType("n1-standard-1");
		//options.setWindowDuration(60);
		options.setSubnetwork("regions/europe-west4/subnetworks/subnet-uc1-3");
		options.setStreaming(true);
		options.setRunner(DataflowRunner.class);
		run(options);
	}

	/**
	 * A DoFn acccepting Json and outputing CommonLog with Beam Schema
	 */
	  public static class JsonToCommonLog extends DoFn<String, CommonLog> {

         private static final long serialVersionUID = 1L;
        
         public static PCollectionTuple process(PCollection<String> input) throws Exception {
         return input.apply("JsonToCommonLog", ParDo.of(new DoFn<String, CommonLog>() {
        
         private static final long serialVersionUID = 1L;
        
         @ProcessElement
         public void processElement(@Element String record, ProcessContext context) {
        
         try {
         Gson gson = new Gson();
         CommonLog commonLog = gson.fromJson(record, CommonLog.class);
         context.output(VALID_MSG, commonLog);
         } catch (Exception e) {
         e.printStackTrace();
         context.output(INVALID_MSG, record);
         }
         }
         }).withOutputTags(VALID_MSG, TupleTagList.of(INVALID_MSG)));
         }
         }

	/**
	 * A Beam schema 
	 */
	 public static final Schema pageViewsSchema = Schema.builder().addInt64Field("id").addStringField("name").addStringField("surname")
			.build();

	
	 public static void run(DataflowPipelineOptions options) throws Exception {

		// Create the pipeline
		 Pipeline pipeline = Pipeline.create(options);

		 PCollection<String> commonLogs = pipeline
				.apply("ReadMessage",
						PubsubIO.readStrings()
								.fromSubscription("projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-3"));

		 PCollectionTuple tuple= JsonToCommonLog.process(commonLogs);

         tuple.get(VALID_MSG).apply("CommonLogToJson", ParDo.of(new DoFn<CommonLog, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext context) {
				Gson gsonObj = new Gson();
				String jsonRecord = gsonObj.toJson(context.element());
				context.output(jsonRecord);
			}
		})).apply("JsontoRow", JsonToRow.withSchema(pageViewsSchema))
           .apply("InserttoBigQuery",
                BigQueryIO.<Row>write().to("nttdata-c4e-bde:uc1_3.account").useBeamSchema()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));;
		
		
        /**
		 * Writing INVALID Messages into the DEAD LETTER TOPIC
		 */
		tuple.get(INVALID_MSG).apply("WriteToDLQTopic", PubsubIO.writeStrings().to("projects/nttdata-c4e-bde/topics/uc1-dlq-topic-3"));
		LOG.info("Building pipeline..");
        pipeline.run().waitUntilFinish();
	}
}
