package flint.db;

public class TestcaseULL {
    public static void main(String[] args) {
        // Test cases for ULONGLONG
        ULONGLONG ull1 = new ULONGLONG(1, 2);
        ULONGLONG ull2 = new ULONGLONG(1, 2);
        ULONGLONG ull3 = new ULONGLONG(2, 3);

        System.out.println("ull1: " + ull1);
        System.out.println("ull2: " + ull2);
        System.out.println("ull3: " + ull3);

        System.out.println("ull1 equals ull2: " + ull1.equals(ull2));
        System.out.println("ull1 equals ull3: " + ull1.equals(ull3));

        System.out.println("ull1 compareTo ull2: " + ull1.compareTo(ull2));
        System.out.println("ull1 compareTo ull3: " + ull1.compareTo(ull3));

        stressTest(1000 * 1_000_000); // Stress test with 1 million iterations
        stressTest(1000 * 1_000_000); // Stress test with 1 million iterations
    }

    static void stressTest(int repeat) {
        IO.StopWatch watch = new IO.StopWatch();
        for (int i = 0; i < repeat; i++) {
            new ULONGLONG(i, i + 1);
            // System.out.println("Stress test " + i + ": " + ull);
        }
        System.out.println("Stress test completed in: " + watch.elapsed() + " ms");
    }
}
