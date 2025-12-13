package flint.db;

/**
 * Logger interface for table operations
 * 
 * Provides logging capabilities for tracking table operations and errors.
 */
public interface Logger {
    /**
     * Log informational message
     * 
     * @param o Message to log
     */
    void log(String fmt, Object... args);

    /**
     * Log error message
     * 
     * @param o Error message to log
     */
    void error(String fmt, Object... args);


    /**
	 * Create null logger that discards log messages but prints errors
	 * 
	 * @return Logger instance that silently discards log messages
	 */
    public static final class NullLogger implements Logger {
        @Override
        public void log(String fmt, Object... args) {
        }

        @Override
        public void error(String fmt, Object... args) {
            System.err.printf(fmt, args);
        }
    }

    public static final class DefaultLogger implements Logger {
        private final java.util.logging.Logger LOGGER;
    
        public DefaultLogger(String name) {
            LOGGER = java.util.logging.Logger.getLogger(name);
        }

        @Override
        public void log(String fmt, Object... args) {
            // https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html
            if (LOGGER.isLoggable(java.util.logging.Level.FINE))
                LOGGER.log(java.util.logging.Level.FINE, String.format(fmt, args));
        }

        @Override
        public void error(String fmt, Object... args) {
            LOGGER.log(java.util.logging.Level.SEVERE, String.format(fmt, args));
        }
    }
}
