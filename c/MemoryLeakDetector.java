
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class MemoryLeakDetector {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java MemoryLeakDetector <mtrace_log_file>");
            System.exit(2);
        }
        try {
            MemoryLeakDetector detector = new MemoryLeakDetector(new File(args[0]));
            detector.detect();
        } catch (IOException e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private final File logfile;

    public MemoryLeakDetector(File logfile) {
        this.logfile = logfile;
    }

    public void detect() throws IOException {
        // Patterns based on debug.c log format
        // Examples:
        // + MALLOC 0x60000085cdf0, 16, 2, ./src/variant.c:102 variant_string_set
        // + CALLOC 0x600003613300, 256, 256, ./src/storage.c:225 storage_read
        // + STRDUP 0x60000085cfe0 24, ./src/foo.c:10 foo
        // + REALLOC 0xABC, 32 <= 0xDEF, 16, ./src/file.c:123 func
        // - FREE 0x60000085cdf0, 16, ./src/variant.c:15 variant_free
        Pattern pMalloc = Pattern.compile("^\\+\\s+MALLOC\\s+(0x[0-9a-fA-F]+),\\s*(\\d+),\\s*(\\d+),\\s*(.+?):(\\d+)\\s+(.+)$");
        Pattern pCalloc = Pattern.compile("^\\+\\s+CALLOC\\s+(0x[0-9a-fA-F]+),\\s*(\\d+),\\s*(\\d+),\\s*(.+?):(\\d+)\\s+(.+)$");
        Pattern pStrdup = Pattern.compile("^\\+\\s+STRDUP\\s+(0x[0-9a-fA-F]+)\\s+(\\d+),\\s*(.+?):(\\d+)\\s+(.+)$");
        Pattern pRealloc = Pattern.compile("^\\+\\s+REALLOC\\s+(0x[0-9a-fA-F]+),\\s*(\\d+)\\s*<=\\s*(0x[0-9a-fA-F]+),\\s*(\\d+),\\s*(.+?):(\\d+)\\s+(.+)$");
        Pattern pFree = Pattern.compile("^-\\s+FREE\\s+(0x[0-9a-fA-F]+),\\s*(\\d+),\\s*(.+?):(\\d+)\\s+(.+)$");

        Map<Long, Allocation> live = new HashMap<>();
        List<FreeEvent> unmatchedFrees = new ArrayList<>();
        long lineNo = 0;

        for (String line : Files.readAllLines(logfile.toPath(), StandardCharsets.UTF_8)) {
            lineNo++;
            line = line.trim();
            if (line.isEmpty()) continue;

            Matcher m;
            if ((m = pMalloc.matcher(line)).matches()) {
                long ptr = parsePtr(m.group(1));
                long usable = Long.parseLong(m.group(2));
                // long req = Long.parseLong(m.group(3)); // not used
                Site site = new Site(m.group(4), Integer.parseInt(m.group(5)), m.group(6));
                live.put(ptr, new Allocation(ptr, usable, site, "+MALLOC", lineNo));
                continue;
            }
            if ((m = pCalloc.matcher(line)).matches()) {
                long ptr = parsePtr(m.group(1));
                long usable = Long.parseLong(m.group(2));
                // long req = Long.parseLong(m.group(3)); // not used
                Site site = new Site(m.group(4), Integer.parseInt(m.group(5)), m.group(6));
                live.put(ptr, new Allocation(ptr, usable, site, "+CALLOC", lineNo));
                continue;
            }
            if ((m = pStrdup.matcher(line)).matches()) {
                long ptr = parsePtr(m.group(1));
                long usable = Long.parseLong(m.group(2));
                Site site = new Site(m.group(3), Integer.parseInt(m.group(4)), m.group(5));
                live.put(ptr, new Allocation(ptr, usable, site, "+STRDUP", lineNo));
                continue;
            }
            if ((m = pRealloc.matcher(line)).matches()) {
                long newPtr = parsePtr(m.group(1));
                long newUsable = Long.parseLong(m.group(2));
                long oldPtr = parsePtr(m.group(3));
                // long oldUsable = Long.parseLong(m.group(4)); // not used
                Site site = new Site(m.group(5), Integer.parseInt(m.group(6)), m.group(7));
                // realloc semantics: old pointer is freed, new pointer allocated (possibly same address)
                // Remove old if present
                live.remove(oldPtr);
                live.put(newPtr, new Allocation(newPtr, newUsable, site, "+REALLOC", lineNo));
                continue;
            }
            if ((m = pFree.matcher(line)).matches()) {
                long ptr = parsePtr(m.group(1));
                // long usable = Long.parseLong(m.group(2)); // not needed for matching
                Site site = new Site(m.group(3), Integer.parseInt(m.group(4)), m.group(5));
                Allocation removed = live.remove(ptr);
                if (removed == null) {
                    unmatchedFrees.add(new FreeEvent(ptr, site, lineNo));
                }
                continue;
            }
            // ignore other lines (stats, etc.)
        }

        // Group remaining live allocations by site (allocation site)
        Map<Site, Summary> bySite = new LinkedHashMap<>();
        for (Allocation a : live.values()) {
            bySite.computeIfAbsent(a.site, k -> new Summary()).add(a.usableSize, 1);
        }

        // Print report
        if (bySite.isEmpty()) {
            System.out.println("No leaks detected. All allocations were freed.");
        } else {
            System.out.println("Leaked allocations by site (file:line func):");
            // sort by total bytes desc
            List<Map.Entry<Site, Summary>> entries = new ArrayList<>(bySite.entrySet());
            entries.sort((a, b) -> Long.compare(b.getValue().bytes, a.getValue().bytes));
            for (Map.Entry<Site, Summary> e : entries) {
                Site s = e.getKey();
                Summary sum = e.getValue();
                System.out.printf("- %s:%d %s — %d blocks, %d bytes%n", s.file, s.line, s.func, sum.blocks, sum.bytes);
            }
            // Optionally show a few example pointers
            System.out.println();
            System.out.println("Examples of leaked pointers (up to 10):");
            int shown = 0;
            for (Allocation a : live.values()) {
                System.out.printf("  * ptr=%s size=%d at %s:%d %s (op=%s line#%d)%n",
                        toHex(a.ptr), a.usableSize, a.site.file, a.site.line, a.site.func, a.op, a.lineNo);
                if (++shown >= 10) break;
            }
        }

        if (!unmatchedFrees.isEmpty()) {
            System.out.println();
            System.out.println("Warning: unmatched FREE events (double-free or free of unknown pointer):");
            for (FreeEvent fe : unmatchedFrees) {
                System.out.printf("  - FREE of ptr=%s at %s:%d %s (line#%d) without matching allocation%n",
                        toHex(fe.ptr), fe.site.file, fe.site.line, fe.site.func, fe.lineNo);
            }
        }

    }

    // Helpers and data classes
    private static long parsePtr(String s) {
        if (s.startsWith("0x") || s.startsWith("0X")) {
            return Long.parseUnsignedLong(s.substring(2), 16);
        }
        return Long.parseUnsignedLong(s, 16);
    }

    private static String toHex(long v) {
        return "0x" + Long.toHexString(v);
    }

    private static final class Site {
        final String file;
        final int line;
        final String func;
        Site(String file, int line, String func) {
            this.file = file;
            this.line = line;
            this.func = func;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Site)) return false;
            Site s = (Site) o;
            return line == s.line && Objects.equals(file, s.file) && Objects.equals(func, s.func);
        }
        @Override public int hashCode() {
            return Objects.hash(file, line, func);
        }
        @Override public String toString() {
            return file + ":" + line + " " + func;
        }
    }

    private static final class Allocation {
        final long ptr;
        final long usableSize;
        final Site site;
        final String op;
        final long lineNo;
        Allocation(long ptr, long usableSize, Site site, String op, long lineNo) {
            this.ptr = ptr;
            this.usableSize = usableSize;
            this.site = site;
            this.op = op;
            this.lineNo = lineNo;
        }
    }

    private static final class Summary {
        long bytes;
        long blocks;
        void add(long b, long c) { this.bytes += b; this.blocks += c; }
    }

    private static final class FreeEvent {
        final long ptr;
        final Site site;
        final long lineNo;
        FreeEvent(long ptr, Site site, long lineNo) { this.ptr = ptr; this.site = site; this.lineNo = lineNo; }
    }
}