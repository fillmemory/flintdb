package flint.db;

import java.io.File;
import java.util.Comparator;

public class BPlusTreeBenchmark {
    public static void main(String[] args) throws Exception {
        try(var closer = new IO.Closer()) {
            var file = new File("temp/bench_bptree.tree");
            var sw = new IO.StopWatch();
            file.delete();
            file.getParentFile().mkdirs();

            var tree = new BPlusTree(file, true, Storage.TYPE_DEFAULT, 32*1024*1024, 100_000, new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return Long.compare(o1, o2);
                }
            }, WAL.NONE);
            closer.register(tree);

            final int N = 1024*1024*1;
            for (int i = 0; i < N; i++) {
                tree.put((long) i);
            }

            System.out.printf("Inserted %d keys, elapsed time: %s, ops/sec: %s\n", N, sw.elapsed(), sw.ops((long) N));
        }
    }
}
