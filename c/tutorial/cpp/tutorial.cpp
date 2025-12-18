#include "flintdbcpp.h"

#include <iostream>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

using namespace flintdbcpp;

// RAII guard for automatic cleanup (like Rust's Drop or Go's defer)
struct CleanupGuard {
    ~CleanupGuard() {
        flintdb_cleanup(nullptr);
    }
};

static int compare_by_value(const void* /*ctx*/, const flintdb_row* a, const flintdb_row* b) {
    char* err = nullptr;
    i32 va = a->i32_get(a, 0, &err);
    i32 vb = b->i32_get(b, 0, &err);
    return va - vb;
}

static void ensure_temp_dir() {
    if (access("./temp", F_OK) == -1) {
        mkdir("./temp", 0755);
    }
}

static int tutorial_table_create() {
    std::cout << "--- Running tutorial_table_create ---\n";

    const std::string tablename = "./temp/tutorial_customer.flintdb";
    flintdb_table_drop(tablename.c_str(), nullptr);

    Meta mt(tablename);
    mt.add_column("id", VARIANT_INT64, 0, 0, SPEC_NOT_NULL, "0", "PRIMARY KEY");
    mt.add_column("name", VARIANT_STRING, 50, 0, SPEC_NOT_NULL, "", "Customer name");
    mt.add_column("age", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "0", "Customer age");

    mt.add_index(PRIMARY_NAME, nullptr, {"id"});
    mt.add_index("ix_age", nullptr, {"age"});

    std::cout << "Table schema SQL:\n" << mt.to_sql_string() << "\n\n";

    auto tbl = Table::create(tablename, FLINTDB_RDWR, mt);

    std::cout << "Inserting 3 rows...\n";
    for (int i = 0; i < 3; i++) {
        Row r(mt);
        r.set_i64(0, i + 1);
        r.set_string(1, "Customer " + std::to_string(i + 1));
        r.set_i32(2, 30 + i);

        if (!r.validate()) {
            throw Error("row validation failed");
        }

        i64 rowid = tbl.apply(r, false);
        if (rowid < 0) {
            throw Error("table->apply returned negative rowid");
        }
    }

    std::cout << "Successfully created table and inserted data.\n\n";
    return 0;
}

static int tutorial_table_find() {
    std::cout << "--- Running tutorial_table_find ---\n";

    const std::string tablename = "./temp/tutorial_customer.flintdb";
    auto tbl = Table::open(tablename, FLINTDB_RDONLY);

    std::cout << "Finding rows where age >= 31:\n";
    for (RowView r : tbl.rows("WHERE age >= 31")) {
        flintdb_print_row(r.raw());
    }

    std::cout << "\nSuccessfully found and read data.\n\n";
    return 0;
}

static int tutorial_table_update_delete() {
    std::cout << "--- Running tutorial_table_update_delete ---\n";

    const std::string tablename = "./temp/tutorial_customer.flintdb";
    auto tbl = Table::open(tablename, FLINTDB_RDWR);

    std::cout << "Finding and updating Customer with age = 30:\n";
    {
        auto c = tbl.find("WHERE age = 30");
        i64 rowid = c.next();
        if (rowid > -1) {
            const flintdb_row* old_row = tbl.read(rowid);
            std::cout << "Before update:\n";
            flintdb_print_row(old_row);

            auto* mt = const_cast<flintdb_meta*>(tbl.meta());
            RowView oldv(old_row);
            Row new_row(mt);
            new_row.set_i64("id", oldv.get_i64("id"));
            new_row.set_string("name", "Updated Customer");
            new_row.set_i32("age", 35);

            tbl.apply_at(rowid, new_row);

            std::cout << "After update:\n";
            const flintdb_row* updated = tbl.read(rowid);
            flintdb_print_row(updated);
        }
    }

    std::cout << "\nDeleting Customer with id = 3:\n";
    {
        auto c = tbl.find("WHERE id = 3");
        i64 rowid = c.next();
        if (rowid > -1) {
            tbl.delete_at(rowid);
            std::cout << "Successfully deleted row at rowid " << rowid << "\n";
        }
    }

    std::cout << "\nRemaining customers:\n";
    {
        for (RowView r : tbl.rows("")) {
            flintdb_print_row(r.raw());
        }
    }

    std::cout << "\nSuccessfully updated and deleted rows.\n\n";
    return 0;
}

static int tutorial_tsv_create() {
    std::cout << "--- Running tutorial_tsv_create ---\n";

    const std::string filepath = "./temp/tutorial_products.tsv";
    flintdb_genericfile_drop(filepath.c_str(), nullptr);

    Meta mt(filepath);
    mt.set_format("tsv");
    mt.set_delimiter('\t');

    mt.add_column("product_id", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "", "");
    mt.add_column("product_name", VARIANT_STRING, 100, 0, SPEC_NOT_NULL, "", "");
    mt.add_column("price", VARIANT_DOUBLE, 0, 0, SPEC_NOT_NULL, "", "");

    auto f = GenericFile::create(filepath, FLINTDB_RDWR, mt);

    std::cout << "Writing 3 rows to TSV...\n";
    for (int i = 0; i < 3; i++) {
        Row r(mt);
        r.set_i32("product_id", 101 + i);
        r.set_string("product_name", std::string("Product-") + static_cast<char>('A' + i));
        r.set_f64("price", 9.99 * (i + 1));
        f.write(r);
    }

    std::cout << "Successfully created TSV file.\n\n";
    return 0;
}

static int tutorial_tsv_find() {
    std::cout << "--- Running tutorial_tsv_find ---\n";

    const std::string filepath = "./temp/tutorial_products.tsv";
    auto f = GenericFile::open(filepath, FLINTDB_RDONLY);

    std::cout << "Reading rows from TSV:\n";
    for (auto* r : f.rows("WHERE product_id >= 102")) {
        flintdb_print_row(r);
    }

    std::cout << "\nSuccessfully read from TSV file.\n\n";
    return 0;
}

static int tutorial_filesort() {
    std::cout << "--- Running tutorial_filesort ---\n";

    const std::string filepath = "./temp/tutorial_sort.dat";
    Meta mt(filepath);
    mt.add_column("value", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "0", "Sort value");
    mt.add_column("label", VARIANT_STRING, 20, 0, SPEC_NOT_NULL, "", "Label");

    FileSort fs(filepath, mt);

    std::cout << "Adding unsorted rows...\n";
    int values[] = {5, 2, 8, 1, 9, 3};
    for (int v : values) {
        Row r(mt);
        r.set_i32(0, v);
        r.set_string(1, "Item-" + std::to_string(v));
        fs.add(r);
    }

    fs.sort(compare_by_value, nullptr);

    std::cout << "Reading sorted rows:\n";
    i64 count = fs.rows();
    for (i64 i = 0; i < count; i++) {
        Row r = fs.read(i);
        flintdb_print_row(r.raw());
    }

    std::cout << "\nSuccessfully sorted " << count << " rows.\n\n";
    return 0;
}

static int tutorial_aggregate() {
    std::cout << "--- Running tutorial_aggregate ---\n";

    const std::string tablename = "./temp/tutorial_sales.flintdb";
    flintdb_table_drop(tablename.c_str(), nullptr);

    Meta mt(tablename);
    mt.add_column("product", VARIANT_STRING, 20, 0, SPEC_NOT_NULL, "", "Product name");
    mt.add_column("category", VARIANT_STRING, 20, 0, SPEC_NOT_NULL, "", "Category");
    mt.add_column("quantity", VARIANT_INT32, 0, 0, SPEC_NOT_NULL, "0", "Quantity sold");
    mt.add_column("price", VARIANT_DOUBLE, 0, 0, SPEC_NOT_NULL, "0.0", "Unit price");
    mt.add_index(PRIMARY_NAME, nullptr, {"product"});

    auto tbl = Table::create(tablename, FLINTDB_RDWR, mt);

    struct Sale {
        const char* product;
        const char* category;
        i32 quantity;
        f64 price;
    };
    std::vector<Sale> sales = {
        {"Apple", "Fruit", 10, 1.50},
        {"Banana", "Fruit", 15, 0.80},
        {"Carrot", "Vegetable", 8, 1.20},
        {"Tomato", "Vegetable", 12, 2.00},
        {"Orange", "Fruit", 7, 1.80},
    };

    std::cout << "Inserting sales data...\n";
    for (const auto& s : sales) {
        Row r(mt);
        r.set_string(0, s.product);
        r.set_string(1, s.category);
        r.set_i32(2, s.quantity);
        r.set_f64(3, s.price);
        tbl.apply(r, false);
    }

    char* e = nullptr;
    auto* gb0 = groupby_new("category", "category", VARIANT_STRING, &e);
    throw_if_error("groupby_new", e);

    flintdb_aggregate_condition no_cond{};
    auto* f0 = flintdb_func_count("*", "count", VARIANT_INT64, no_cond, &e);
    throw_if_error("flintdb_func_count", e);
    auto* f1 = flintdb_func_sum("quantity", "total_quantity", VARIANT_INT32, no_cond, &e);
    throw_if_error("flintdb_func_sum", e);
    auto* f2 = flintdb_func_avg("price", "avg_price", VARIANT_DOUBLE, no_cond, &e);
    throw_if_error("flintdb_func_avg", e);

    Aggregate agg = Aggregate::create(
        "sales_by_category",
        std::vector<flintdb_aggregate_groupby*>{gb0},
        std::vector<flintdb_aggregate_func*>{f0, f1, f2});

    {
        for (RowView r : tbl.rows("")) {
            agg.row(r.raw());
        }
    }

    std::cout << "\nAggregation results (by category):\n";
    auto results = agg.compute();
    for (auto& r : results) {
        flintdb_print_row(r.raw());
    }

    std::cout << "\nSuccessfully performed aggregation.\n\n";
    return 0;
}

static int tutorial_flintdb_sql_exec() {
    std::cout << "--- Running tutorial_flintdb_sql_exec ---\n";

    const std::string tablename = "./temp/tutorial_employees.flintdb";
    flintdb_table_drop(tablename.c_str(), nullptr);

    std::cout << "Creating table with API...\n";
    Meta mt(tablename);
    mt.add_column("id", VARIANT_INT64, 0, 0, SPEC_NOT_NULL, "0", "Employee ID");
    mt.add_column("name", VARIANT_STRING, 50, 0, SPEC_NOT_NULL, "", "Employee name");
    mt.add_column("department", VARIANT_STRING, 30, 0, SPEC_NOT_NULL, "", "Department");
    mt.add_column("salary", VARIANT_DOUBLE, 0, 0, SPEC_NOT_NULL, "0.0", "Salary");
    mt.add_index(PRIMARY_NAME, nullptr, {"id"});
    {
        auto tbl = Table::create(tablename, FLINTDB_RDWR, mt);
        (void)tbl;
    }

    std::cout << "Executing SQL INSERT...\n";
    std::vector<std::string> insert_sqls = {
        "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (1, 'Alice', 'Engineering', 75000.0)",
        "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (2, 'Bob', 'Sales', 65000.0)",
        "INSERT INTO ./temp/tutorial_employees.flintdb VALUES (3, 'Charlie', 'Engineering', 80000.0)",
    };
    i64 total_affected = 0;
    for (const auto& s : insert_sqls) {
        auto r = sql_exec(s);
        total_affected += r.affected();
    }
    std::cout << "Affected rows: " << total_affected << "\n";

    std::cout << "\nExecuting SQL SELECT...\n";
    const std::string select_sql =
        "SELECT * FROM ./temp/tutorial_employees.flintdb WHERE department = 'Engineering'";
    auto result = sql_exec(select_sql);

    std::cout << "Columns: ";
    for (int i = 0; i < result.column_count(); i++) {
        const char* n = result.column_name(i);
        std::cout << (n ? n : "");
        if (i < result.column_count() - 1) std::cout << ", ";
    }
    std::cout << "\n";

    for (auto* r : result.rows()) {
        flintdb_print_row(r);
    }

    std::cout << "\nSuccessfully executed SQL statements.\n\n";
    return 0;
}

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    // Setup automatic cleanup on scope exit (RAII)
    CleanupGuard cleanup_guard;

    try {
        ensure_temp_dir();

        if (tutorial_table_create() != 0) return 1;
        if (tutorial_table_find() != 0) return 1;
        if (tutorial_table_update_delete() != 0) return 1;

        if (tutorial_tsv_create() != 0) return 1;
        if (tutorial_tsv_find() != 0) return 1;

        if (tutorial_filesort() != 0) return 1;
        if (tutorial_aggregate() != 0) return 1;
        if (tutorial_flintdb_sql_exec() != 0) return 1;

        std::cout << "All tutorial steps completed successfully.\n";
        
        // Cleanup will be called automatically when cleanup_guard goes out of scope
        
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
}