/**
 * WebUI - A web-based user interface for FlintDB
 * Provides HTTP server functionality to execute SQL queries via web interface
 */
package flint.db;

import java.awt.Desktop;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpServer;

/**
 * WebUI class provides a web-based interface for FlintDB
 * Starts an HTTP server and handles SQL query execution via REST API
 */
public final class WebUI {

	/**
	 * Main entry point for WebUI application
	 * Starts the web server on port 3333 and opens the default browser
	 */
	public static void main(String[] args) throws Exception {
		final int port = 3333; // Default port for web server
		final String URL = "http://localhost:" + port;
		final WebUI webui = new WebUI();
		webui.run(port);

		System.out.println(URL);
		// System.out.println("PWD : " + new File(".").getCanonicalPath());
		
		// Automatically open the web browser if desktop is supported
		if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
			final String osname = System.getProperty("os.name");
			// System.out.println(osname);
			
			// Handle different operating systems for opening browser
			if (osname.toLowerCase().contains("mac os x")) {
				// macOS - use 'open' command
				final ProcessBuilder pb = new ProcessBuilder();
				pb.command("open", URL);
				pb.start();
			} else {
				// Linux and other systems - use default desktop browser
				Desktop.getDesktop().browse(new URI(URL));
				// LINUX xdg-open http://stackoverflow.com
			}
		}
	}

	/**
	 * Starts the HTTP server and configures request handlers
	 * @param port The port number to bind the server to
	 * @throws IOException if server cannot be started
	 */
	private void run(final int port) throws IOException {
		final InetSocketAddress address = new InetSocketAddress(port);
		final HttpServer server = HttpServer.create(address, 0);
		// Configure JSON serializer with date formatting and pretty printing
		final Gson g = new GsonBuilder() //
				.setDateFormat("yyyy-MM-dd HH:mm:ss") //
				.setPrettyPrinting() //
				.create();

		// Create context handler for all requests
		server.createContext("/", (exchange) -> {
			final String path = exchange.getRequestURI().getPath();

			try (final IO.Closer CLOSER = new IO.Closer()) {
				final PrintStream out = CLOSER.register(new PrintStream(exchange.getResponseBody()));
				final IO.StopWatch watch = new IO.StopWatch();

				if ("GET".equals(exchange.getRequestMethod())) {
					LOG(exchange.getRemoteAddress().getAddress().getHostAddress() + " -\t-\t" + exchange.getRequestURI());
					// Handle GET requests - serve HTML interface
					exchange.getResponseHeaders().add("Content-Type", "text/html;charset=UTF-8");
	
					final File html = new File("temp/webui.html");
					System.out.println(exchange.getRemoteAddress().getAddress().getHostAddress() + "	" + path);
					// Load HTML from file or embedded resource
					final InputStream instream = CLOSER.register(html.exists() ? new java.io.FileInputStream(html) : getClass().getResourceAsStream("/webui.html"));

					exchange.sendResponseHeaders(200, html.length());
					IO.pipe(instream, out);
					LOG(exchange.getRemoteAddress().getAddress().getHostAddress() + " 200\t" + watch.elapsed() + "ms" + "\t" + exchange.getRequestURI());
				} else {
					// Handle POST requests - execute SQL queries and return JSON
					exchange.getResponseHeaders().add("Content-Type", "application/json;charset=UTF-8");
	
					try {
						// Parse incoming JSON request
						final var ir = CLOSER.register(new InputStreamReader(exchange.getRequestBody(), "UTF-8"));
						final var input = g.fromJson(ir, INPUT.class);
						final String q = input.q;
	
						LOG(exchange.getRemoteAddress().getAddress().getHostAddress() + " -\t-\t" + q);
						// Execute SQL query and capture output
						final ByteArrayOutputStream stream = new ByteArrayOutputStream();
						execute(new PrintStream(stream), new String[] { q });
	
						final byte[] bb = stream.toByteArray();
						exchange.sendResponseHeaders(200, bb.length);
	
						out.write(bb);

						LOG(exchange.getRemoteAddress().getAddress().getHostAddress() + " 200\t" + watch.elapsed() + "ms" + "\t" + q);
					} catch (Throwable ex) {
						// ex.printStackTrace();
						
						final ByteArrayOutputStream stream = new ByteArrayOutputStream();
						generateJsonResponse(ex, new PrintStream(stream));
						final byte[] bb = stream.toByteArray();
						exchange.sendResponseHeaders(503, bb.length);
						LOG(exchange.getRemoteAddress().getAddress().getHostAddress() + " 503\t" + watch.elapsed() + "ms" + "\t" + exchange.getRequestURI() + "\t"+ ex.getMessage());
						out.write(bb);
					}
				}
			}
		});

		// Start the server with default executor
		server.setExecutor(null);
		server.start();
	}

	private static void LOG(final String s) {
		System.out.println(LocalDateTime.now().format(Row.DATE_TIME_FORMAT_MS3) + " " + s);
	}

	/**
	 * Input data structure for JSON requests
	 * Contains SQL query and related parameters
	 */
	static final class INPUT {
		String u; // URL/connection string
		String q; // SQL query
		Object[] a; // Arguments/parameters
		String f; // Format/flags
	}

	/**
	 * Executes SQL commands and returns the number of affected rows
	 * Supports SELECT, DESC, META, UPDATE, DELETE, INSERT, SHOW, and DROP statements
	 * @param out PrintStream to output results
	 * @param args Command line arguments including SQL statement and options
	 * @return Number of rows affected or processed, -1 on error
	 * @throws Exception if execution fails
	 */
	public static long execute(final PrintStream out, final String[] args) throws Exception {
		String sql = null;
		boolean head = true; // Include column headers in output
		boolean rownum = false; // Include row numbers in output

		// Parse command line arguments
		for (int i = 0; i < args.length; i++) {
			final String s = args[i];
			switch (s) {
			case "-help":
				return 0L;
			case "-nohead":
				head = false;
				break;
			case "-rownum":
				rownum = true;
				break;
			default:
				if (sql == null)
					sql = s;
				break;
			}
		}

		if (args.length == 0) {
			return 0L;
		}

		if (sql == null)
			return perror("sql must be specified " + java.util.Arrays.toString(args));

		final PrintStream ps = out;

		try {
			// Execute SQL using SQLExec
			SQLResult result = SQLExec.execute(sql);
			
			if (result == null) {
				generateJsonResponse("Failed to execute SQL", ps);
				return -1;
			}

			// Handle different result types
			if (result.getCursor() != null) {
				// SELECT query with row cursor
				String[] columns = result.getColumns();
				if (columns == null || columns.length == 0) {
					generateJsonResponse("No column information in result", ps);
					return 0;
				}
				
				return printJsonResults(ps, result, head, rownum);
			} else {
				// Non-SELECT query (INSERT, UPDATE, DELETE, etc.)
				long affected = result.getAffected();
				return generateJsonResponse(affected, ps);
			}
		} catch (Exception e) {
			return generateJsonResponse(e, ps);
		}
	}

	/**
	 * Prints query results in JSON format
	 */
	private static long printJsonResults(PrintStream ps, SQLResult result, boolean head, boolean rownum) throws Exception {
		String[] columns = result.getColumns();
		Cursor<Row> cursor = result.getCursor();
		JsonFormatter formatter = new JsonFormatter(ps);
		
		if (head) {
			formatter.begin(rownum, columns, null);
		}
		
		long rowCount = 0;
		Row r;
		
		while ((r = cursor.next()) != null) {
			rowCount++;
			Object[] rowData = new Object[columns.length];
			
			for (int i = 0; i < columns.length; i++) {
				Object v = r.get(i);
				rowData[i] = (v != null) ? v : null;
			}
			
			if (rownum) {
				formatter.print(rowCount, rowData);
			} else {
				formatter.print(rowData);
			}
		}
		
		formatter.flush();
		return rowCount;
	}

	final static long generateJsonResponse(final long rows, final PrintStream ps) throws IOException {
		var formatter = new JsonFormatter(ps);
		formatter.begin(false, new String[] { "" }, null);
		formatter.print(1, new Object[] { rows + " rows affected" });
		formatter.flush();
		return rows;
	}

	final static long generateJsonResponse(final String message, final PrintStream ps) throws IOException {
		var formatter = new JsonFormatter(ps);
		formatter.begin(false, new String[] { "" }, null);
		formatter.print(1, new Object[] { message });
		formatter.flush();
		return 1;
	}

	final static long generateJsonResponse(final Throwable e, final PrintStream ps) throws IOException {
		assert e != null;
		java.io.StringWriter sw = new java.io.StringWriter();
		java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        if (e instanceof DatabaseException dbe) {
            pw.print(dbe.getMessage());
        } else {
		    e.printStackTrace(pw);
        }
		pw.flush();
		String stackTrace = sw.toString();
		return generateJsonResponse(stackTrace, ps);
	}

	/**
	 * JsonFormatter - Formats query results as JSON
	 * Implements output formatter interface to output results in JSON format
	 */
	final static class JsonFormatter {
		final List<Object[]> rows = new ArrayList<>(); // Store all result rows
		final PrintStream out;

		public JsonFormatter(final PrintStream ps) {
			this.out = ps;
		}

		public void begin(boolean rownum, String[] columns, byte[] align) throws IOException {
			rows.add(columns); // Add column headers as first row
		}

		public void print(Object[] array) throws IOException {
			rows.add(array); // Add data row
		}

		public void print(long rownum, Object[] array) throws IOException {
			rows.add(array); // Add data row with row number
		}

		public void flush() throws IOException {
			// Convert all rows to JSON and output
			final Gson g = new GsonBuilder() //
					.setDateFormat("yyyy-MM-dd HH:mm:ss") //
					.setPrettyPrinting() //
					.create();
			g.toJson(rows, out);
		}
	}

	/**
	 * Converts absolute file path to relative path from current working directory
	 * @param file The file to relativize
	 * @return Relative path string or absolute path if relativization fails
	 */
	static String relativize(final File file) {
		try {
			// Get current working directory URI
			URI pwd = new File(".").getCanonicalFile().toURI();
			// Return relative path from current directory to the file
			return pwd.relativize(file.getCanonicalFile().toURI()).getPath();
		} catch (Exception e) {
			// If relativization fails, return absolute path
		}
		return file.getAbsolutePath();
	}

	/**
	 * Creates an output stream for a file, with GZIP compression for .gz files
	 * @param f The output file
	 * @return PrintStream for writing to the file
	 * @throws IOException if file cannot be created
	 */
	static java.io.PrintStream ostream(final File f) throws IOException {
		// Use GZIP compression for .gz files
		if (f.getName().endsWith(".gz"))
			return new java.io.PrintStream(new java.util.zip.GZIPOutputStream(new java.io.FileOutputStream(f), 65536));
		return new java.io.PrintStream(f);
	}

	/**
	 * Validates file existence and accessibility
	 * @param f The file to validate
	 * @return 0 if file is valid, -1 if invalid
	 */
	static int perror(final File f) {
		if (f == null)
			return perror("file is not specified");
		if (!f.exists())
			return perror(f.getAbsolutePath() + " not a file");
		return 0;
	}

	/**
	 * Prints error message to stderr
	 * @param s The error message
	 * @return Always returns -1 to indicate error
	 */
	static int perror(final String s) {
		System.err.println(s);
		return -1;
	}
}
