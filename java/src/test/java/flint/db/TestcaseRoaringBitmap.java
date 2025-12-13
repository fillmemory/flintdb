package flint.db;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;

public final class TestcaseRoaringBitmap {
    public static void main(String[] args) throws Exception {
        // test1();
        test2();
        // test3();
    }

    static void test1() throws Exception {
        // 기본 사용 예제
        RoaringBitmap rb = new RoaringBitmap();
        rb.add(1);
        rb.add(3);
        rb.addRange(100, 110); // [100..109]
        rb.addRange(1_000_000, 1_000_100);

        System.out.println("cardinality=" + rb.cardinality());
        System.out.println("contains(3)=" + rb.contains(3));
        System.out.println("contains(2)=" + rb.contains(2));

        // rank/select
        System.out.println("rank(3) (<=3 개수) = " + rb.rank(3));
        System.out.println("select(0) = " + rb.select(0));
        System.out.println("select(rank(3)-1) = " + rb.select(rb.rank(3) - 1));

        // 순회 (처음 몇 개만 출력)
        System.out.print("iterate first 15: ");
        PrimitiveIterator.OfInt it = rb.iterator();
        for (int i = 0; i < 15 && it.hasNext(); i++) {
            System.out.print(it.nextInt());
            if (i < 14 && it.hasNext()) System.out.print(", ");
        }
        System.out.println();

        // 집합 연산 예제
        RoaringBitmap other = new RoaringBitmap();
        other.addRange(105, 115); // 105..114
        other.add(3);
        RoaringBitmap uni = rb.or(other);
        RoaringBitmap inter = rb.and(other);
        RoaringBitmap diff = rb.andNot(other);
        System.out.println("union.cardinality=" + uni.cardinality());
        System.out.println("inter.cardinality=" + inter.cardinality());
        System.out.println("diff.cardinality=" + diff.cardinality());

        // byte[] 직렬화/역직렬화
        byte[] bytes = rb.toByteArray();
        RoaringBitmap fromBytes = RoaringBitmap.fromByteArray(bytes);
        System.out.println("roundtrip(bytes) equals=" + rb.equals(fromBytes));

        // 파일로 저장/로드 (temp 디렉토리 사용)
        Path outDir = Path.of("temp");
        Files.createDirectories(outDir);
        Path file = outDir.resolve("roaring_bitmap.rbm");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            rb.writeTo(dos);
        }
        // 사람이 읽기 쉬운 TSV도 함께 저장 (한 줄에 한 user_id)
        Path fileTsv = outDir.resolve("roaring_bitmap.tsv");
        try (BufferedWriter bw = Files.newBufferedWriter(fileTsv, StandardCharsets.UTF_8)) {
            PrimitiveIterator.OfInt bit = rb.iterator();
            while (bit.hasNext()) {
                bw.write(Integer.toString(bit.nextInt()));
                bw.newLine();
            }
        }
        RoaringBitmap fromFile;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file.toFile()))) {
            fromFile = RoaringBitmap.readFrom(dis);
        }
        System.out.println("roundtrip(file) equals=" + rb.equals(fromFile));

    }

    static void test2() throws Exception {

        // ==============================
        // 일별 사용자 로그 집계/저장/7일 합치기
        // ==============================
        System.out.println("\n== Daily Active Users demo ==");

        Path auDir = Path.of("temp", "active_users");
        Files.createDirectories(auDir);

        // 데모 데이터 생성 파라미터
        int totalDays = 10; // 총 10일치 생성
        int baselineUsers = 1000; // 매일 공통으로 활동하는 사용자 수 (1..baselineUsers)
        int dailyExtraMin = 2000; // 하루 추가 활동 사용자 최소
        int dailyExtraMax = 4000; // 하루 추가 활동 사용자 최대
        int userIdMax = 1_000_000; // 사용자 ID 상한
        Random rnd = new Random(42); // 재현 가능성
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");

        // 일별 로그 생성 및 파일 저장
        List<String> days = new ArrayList<>();
        for (int i = totalDays - 1; i >= 0; i--) {
            LocalDate day = LocalDate.now().minusDays(i);
            String dayStr = fmt.format(day);
            days.add(dayStr);

            RoaringBitmap dayRb = new RoaringBitmap();
            // 공통 사용자
            dayRb.addRange(1, baselineUsers + 1);
            // 일별 추가 사용자
            int extra = dailyExtraMin + rnd.nextInt(dailyExtraMax - dailyExtraMin + 1);
            for (int k = 0; k < extra; k++) {
                int id = baselineUsers + 1 + rnd.nextInt(userIdMax - baselineUsers);
                dayRb.add(id);
            }

            Path out = auDir.resolve("active_" + dayStr + ".rbm");
            try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(out.toFile()))) {
                dayRb.writeTo(dos);
            }
            // TSV로도 저장
            Path outTsv = auDir.resolve("active_" + dayStr + ".tsv");
            try (BufferedWriter bw = Files.newBufferedWriter(outTsv, StandardCharsets.UTF_8)) {
                PrimitiveIterator.OfInt dit = dayRb.iterator();
                while (dit.hasNext()) {
                    bw.write(Integer.toString(dit.nextInt()));
                    bw.newLine();
                }
            }
            System.out.println(dayStr + " saved, cardinality=" + dayRb.cardinality());
        }

        // 최근 7일 파일을 읽어 합집합(7일 active users)
        int window = 7;
        days.sort(Comparator.naturalOrder());
        int start = Math.max(0, days.size() - window);
        RoaringBitmap weekly = new RoaringBitmap();
        for (int i = start; i < days.size(); i++) {
            String d = days.get(i);
            Path f = auDir.resolve("active_" + d + ".rbm");
            try (DataInputStream dis = new DataInputStream(new FileInputStream(f.toFile()))) {
                RoaringBitmap day = RoaringBitmap.readFrom(dis);
                weekly = weekly.or(day);
            }
        }
        System.out.println("7-day active users (union) cardinality=" + weekly.cardinality());

        System.out.println("Done.");
    }

    static void test3() throws Exception {
        RoaringBitmap rb = new RoaringBitmap();
        rb.add(10);
        rb.add(20);
        rb.add(30);
        rb.add(30);
        System.out.println("test3: " + rb.cardinality());
    }
}
