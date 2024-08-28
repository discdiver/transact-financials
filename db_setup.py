# Script to generate and populate sales and expenses database tables

from datetime import datetime
import duckdb
from faker import Faker
import pandas as pd

fake = Faker()


def generate_sales_data(num_records: int) -> pd.DataFrame:
    """Generate fake sales data"""
    data = []
    start_date = datetime(2024, 7, 1)
    end_date = datetime(2024, 7, 31)

    for _ in range(num_records):
        date = fake.date_between(start_date=start_date, end_date=end_date)

        product = fake.word()
        quantity = fake.pyint(min_value=1, max_value=100)
        price = round(fake.pyfloat(min_value=10, max_value=1000), 2)
        data.append((date, product, quantity, price))
    return pd.DataFrame(data, columns=["date", "product", "quantity", "price"])


def generate_expenses_data(num_records: int) -> pd.DataFrame:
    """Generate fake expenses data"""
    data = []
    start_date = datetime(2024, 7, 1)
    end_date = datetime(2024, 7, 31)
    expense_categories = [
        "Utilities",
        "Office Supplies",
        "Marketing",
        "Travel",
        "Consulting Fees",
    ]

    for _ in range(num_records):
        date = fake.date_between(start_date=start_date, end_date=end_date)

        category = fake.random_element(expense_categories)
        amount = round(fake.pyfloat(min_value=100, max_value=10000), 2)
        data.append((date, category, amount))
    return pd.DataFrame(data, columns=["date", "category", "amount"])


def create_and_fill_sales_db(sales_data: pd.DataFrame):
    """Create and populate sales database"""
    try:
        with duckdb.connect("sales.duckdb") as conn:
            conn.execute(
                "CREATE TABLE sales (date DATE, product VARCHAR, quantity INTEGER, price DECIMAL(10, 2))"
            )
            conn.execute("INSERT INTO sales SELECT * FROM sales_data")
    except Exception as e:
        print("Error creating sales database:", e)

    print("Sales database created and populated.")


def create_and_fill_expenses_db(expenses_data: pd.DataFrame):
    """Create and populate expenses database"""
    try:
        with duckdb.connect("expenses.duckdb") as conn:
            conn.execute(
                "CREATE TABLE expenses (date DATE, category VARCHAR, amount DECIMAL(7, 2))"
            )
            conn.execute("INSERT INTO expenses SELECT * FROM expenses_data")
    except Exception as e:
        print("Error creating expenses database:", e)

    print("Expenses database created and populated.")


def create_financial_db():
    """Create empty financial database"""
    with duckdb.connect("financial.duckdb") as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS financial_report (date DATE, total_sales DECIMAL(10, 2), total_expenses DECIMAL(10, 2), net_income DECIMAL(10, 2))"
        )
        print("Financial database created.")

    print("All databases have been set up successfully.")


def verify_data():
    """Verify that the data has been loaded correctly"""
    try:
        with duckdb.connect("sales.duckdb") as conn:
            print("\nSample sales data:")
            print(conn.execute("SELECT * FROM sales LIMIT 5").fetchdf())
    except Exception as e:
        print("Error accessing sales data:", e)

    try:
        with duckdb.connect("expenses.duckdb") as conn:
            print("\nSample expenses data:")
            print(conn.execute("SELECT * FROM expenses LIMIT 5").fetchdf())
    except Exception as e:
        print("Error accessing expenses data:", e)

    print("\nDatabases are ready for use in the ETL pipeline.")


if __name__ == "__main__":
    sales_df = generate_sales_data(1000)
    expenses_df = generate_expenses_data(500)
    create_and_fill_sales_db(sales_data=sales_df)
    create_and_fill_expenses_db(expenses_data=expenses_df)
    create_financial_db()
    verify_data()
