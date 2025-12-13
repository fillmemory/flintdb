package flint.db;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.zip.GZIPInputStream;

public class TestcaseTPCH_H2 {
    
    public static void main(String[] args) throws Exception {
        // H2 드라이버 로드
        Class.forName("org.h2.Driver");
        
        String dbPath = "/Users/fillmemory/works/flintdb/temp/h2_test/tpch_h2";
        String gzFilePath = "/Users/fillmemory/works/flintdb/temp/tpch/lineitem.tbl.gz";
        
        // H2 데이터베이스 연결
        String url = "jdbc:h2:" + dbPath + ";AUTO_SERVER=FALSE;DB_CLOSE_ON_EXIT=FALSE;FILE_LOCK=NO";
        Connection conn = DriverManager.getConnection(url, "sa", "");
        
        // 테이블 생성
        createTable(conn);
        
        // 데이터 삽입 성능 테스트
        long startTime = System.currentTimeMillis();
        long rowCount = insertData(conn, gzFilePath);
        long endTime = System.currentTimeMillis();
        
        long elapsedMs = endTime - startTime;
        long elapsedSec = elapsedMs / 1000;
        long opsPerSec = rowCount * 1000 / elapsedMs;
        
        System.out.printf("H2 Database Performance:%n");
        System.out.printf("%,d rows, %dm%ds, %,d ops/sec%n", 
                         rowCount, elapsedSec / 60, elapsedSec % 60, opsPerSec);
        
        // 파일 크기 출력
        printFileInfo(dbPath);
        
        conn.close();
    }
    
    private static void createTable(Connection conn) throws SQLException {
        String createSQL = 
            "CREATE TABLE IF NOT EXISTS tpch_lineitem (" +
            "l_orderkey BIGINT, " +
            "l_partkey BIGINT, " +
            "l_suppkey INTEGER, " +
            "l_linenumber SMALLINT, " +
            "l_quantity DECIMAL(15,2), " +
            "l_extendedprice DECIMAL(15,2), " +
            "l_discount DECIMAL(15,2), " +
            "l_tax DECIMAL(15,2), " +
            "l_returnflag CHAR(1), " +
            "l_linestatus CHAR(1), " +
            "l_shipdate DATE, " +
            "l_commitdate DATE, " +
            "l_receiptdate DATE, " +
            "l_shipinstruct VARCHAR(25), " +
            "l_shipmode VARCHAR(10), " +
            "l_comment VARCHAR(44), " +
            "PRIMARY KEY (l_orderkey, l_linenumber)" +
            ")";
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS tpch_lineitem");
            stmt.execute(createSQL);
            
            // H2 성능 최적화 설정 (H2 2.x 호환)
            stmt.execute("SET WRITE_DELAY 1000"); // 1초 지연
            stmt.execute("SET CACHE_SIZE 65536"); // 64MB 캐시
        }
    }
    
    private static long insertData(Connection conn, String gzFilePath) throws Exception {
        String insertSQL = 
            "INSERT INTO tpch_lineitem VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        
        conn.setAutoCommit(false);
        
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL);
             InputStream fis = new FileInputStream(gzFilePath);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzis))) {
            
            String line;
            long rowCount = 0;
            long batchCount = 0;
            final int BATCH_SIZE = 10000;
            
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\|");
                if (fields.length >= 16) {
                    
                    pstmt.setLong(1, Long.parseLong(fields[0]));
                    pstmt.setLong(2, Long.parseLong(fields[1]));
                    pstmt.setInt(3, Integer.parseInt(fields[2]));
                    pstmt.setShort(4, Short.parseShort(fields[3]));
                    pstmt.setBigDecimal(5, new java.math.BigDecimal(fields[4]));
                    pstmt.setBigDecimal(6, new java.math.BigDecimal(fields[5]));
                    pstmt.setBigDecimal(7, new java.math.BigDecimal(fields[6]));
                    pstmt.setBigDecimal(8, new java.math.BigDecimal(fields[7]));
                    pstmt.setString(9, fields[8]);
                    pstmt.setString(10, fields[9]);
                    pstmt.setDate(11, java.sql.Date.valueOf(fields[10]));
                    pstmt.setDate(12, java.sql.Date.valueOf(fields[11]));
                    pstmt.setDate(13, java.sql.Date.valueOf(fields[12]));
                    pstmt.setString(14, fields[13]);
                    pstmt.setString(15, fields[14]);
                    pstmt.setString(16, fields[15]);
                    
                    pstmt.addBatch();
                    rowCount++;
                    
                    if (rowCount % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        conn.commit();
                        batchCount++;
                        
                        if (batchCount % 100 == 0) {
                            System.out.printf("H2 Progress: %,d rows processed%n", rowCount);
                        }
                    }
                }
            }
            
            // 마지막 배치 처리
            pstmt.executeBatch();
            conn.commit();
            
            return rowCount;
        }
    }
    
    private static void printFileInfo(String dbPath) {
        File dbFile = new File(dbPath + ".mv.db");
        if (dbFile.exists()) {
            System.out.printf("H2 DB file size: %,d bytes%n", dbFile.length());
        }
    }
}
