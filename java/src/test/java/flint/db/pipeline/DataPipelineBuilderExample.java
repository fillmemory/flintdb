package flint.db.pipeline;

import java.util.HashMap;
import java.util.Map;


/**
 * Example of using DataPipeline programmatic API
 */
public class DataPipelineBuilderExample {

    public static void main(String[] args) throws Exception {
        example1_BasicUsage();
        example2_WithVariables();
        example3_WithDateRangeLoop();
        example4_WithHandlerAndSort();
    }

    /**
     * Basic transform: CSV to Parquet
     */
    static void example1_BasicUsage() throws Exception {
        System.out.println("=== Example 1: Basic CSV to Parquet ===");
        
        DataPipeline pipeline = DataPipeline.createBuilder("csv-to-parquet")
            .maxThreads(4)
            .maxErrors(10)
            .variable("d1", "20240101")
            .input()
                .directory("./data/input")
                .pattern(".*\\.csv")
                .formatType("csv")
                .maxDepth(1)
                .done()
            .output()
                .directory("./data/output")
                .filename("{basename}.parquet")
                .column("id", "long").from("user_id").notNull(true).done()
                .column("name", "string").bytes(100).from("user_name").done()
                .column("email", "string").bytes(200).from("email").done()
                .column("age", "int").from("age").defaultValue(0).done()
                .done()
            .build();
        
        pipeline.run(); // Uncomment to execute
        System.out.println("Transformer built successfully\n");
    }

    /**
     * Transform with date variables
     */
    static void example2_WithVariables() throws Exception {
        System.out.println("=== Example 2: With Date Variables ===");
        
        DataPipeline pipeline = DataPipeline.createBuilder("with-variables")
            .maxThreads(2)
            .dateVariable("date", "yyyyMMdd", -1)  // yesterday
            .dateVariable("month", "yyyyMM", 0)     // this month
            .envVariable("home", "HOME")            // environment variable
            .variable("env", "production")
            .input()
                .directory("{home}/logs/{date}")
                .pattern("app_.*\\.log")
                .formatType("tsv")
                .where("level = 'ERROR'")  // Filter only errors
                .done()
            .output()
                .directory("./reports/{month}")
                .filename("errors_{date}.parquet")
                .skipIfExists(true)
                .column("timestamp", "string").bytes(30).from("timestamp").done()
                .column("level", "string").bytes(20).from("level").done()
                .column("message", "string").bytes(500).from("message").done()
                .done()
            .build();
        
        assert pipeline != null;
        // pipeline.run();
        System.out.println("Transformer with variables built successfully\n");
    }

    /**
     * Transform with date range loop (processes multiple days)
     */
    static void example3_WithDateRangeLoop() throws Exception {
        System.out.println("=== Example 3: With Date Range Loop ===");
        
        DataPipeline pipeline = DataPipeline.createBuilder("date-range-loop")
            .maxThreads(4)
            .dateRangeVariable("date", "yyyyMMdd", -29, 0)      // last 30 days
            .dateRangeVariable("dateDash", "yyyy-MM-dd", -29, 0) // same range, different format
            .envVariable("home", "HOME")
            .input()
                .directory("{home}/data/dd/{dateDash}")
                .pattern("(request_page|request_page_act|request_error)\\.gz")
                .formatType("tsv.gz")
                .maxDepth(1)
                .done()
            .output()
                .directory("./temp/transformed/active")
                .filename("app_logs_{date}.parquet")
                .skipIfExists(true)
                .sortBy("timestamp", "customer_id")
                .column("timestamp", "time").from("request_ymd").notNull(true).done()
                .column("customer_id", "long").from("client_id").transform("hash").notNull(true).done()
                .column("session_id", "long").from("session_id").notNull(true).done()
                .column("page_id", "string").bytes(120).from("page_id").defaultValue("").done()
                .done()
            .build();
        
        assert pipeline != null;
        // pipeline.run();
        System.out.println("Transformer with date range loop built successfully\n");
    }

    /**
     * Transform with custom handler and sorting
     */
    static void example4_WithHandlerAndSort() throws Exception {
        System.out.println("=== Example 4: With Handler and Sorting ===");
        
        Map<String, String> handlerParams = new HashMap<>();
        handlerParams.put("param1", "value1");
        handlerParams.put("param2", "value2");
        
        DataPipeline pipeline = DataPipeline.createBuilder("with-handler-sort")
            .maxThreads(8)
            .input()
                .directory("./data/raw")
                .pattern("data_\\d{8}\\.tsv\\.gz")
                .formatType("tsv.gz")
                .maxDepth(2)
                .done()
            .output()
                .directory("./data/processed")
                .filename("output_{basename}.flint")
                .overwriteIfExists(true)
                .sortBy("timestamp", "user_id")  // Sort by these columns
                .column("user_id", "long").from("uid").done()
                .column("user_hash", "long").from("email", "phone").transform("hash").done()
                .column("timestamp", "string").bytes(30).from("created_at").done()
                .column("amount", "decimal").bytes(16).precision(2).from("amount").done()
                .done()
            // .handler(CustomRowHandler.class, handlerParams)  // Add custom handler
            .build();
        
        assert pipeline != null;
        // pipeline.run();
        System.out.println("Transformer with handler and sort built successfully\n");
    }

    // Example custom handler
    /*
    public static class CustomRowHandler implements Transformer.RowHandler {
        private Map<String, String> params;
        
        @Override
        public void init(Map<String, String> params) {
            this.params = params;
            System.out.println("Handler initialized with params: " + params);
        }
        
        @Override
        public void handle(flint.db.Row in, Map<String, String> vars, Map<String, Object> out) {
            // Custom row processing logic
            // Add computed fields to 'out' map
            out.put("computed_field", in.get("some_field") + "_processed");
        }
        
        @Override
        public void close() {
            System.out.println("Handler closed");
        }
    }
    */
}
