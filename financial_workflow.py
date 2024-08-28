from prefect import flow, task
from prefect.transactions import transaction
from prefect.artifacts import create_table_artifact
import pandas as pd
import duckdb


# DuckDB database file names
DB_SALES = "sales.duckdb"
DB_EXPENSES = "expenses.duckdb"
DB_FINANCIAL = "financial.duckdb"


@task
def extract_data():
    """Extract data from DuckDB"""
    with duckdb.connect(DB_SALES) as conn:
        df_sales = conn.execute("SELECT * FROM sales").fetchdf()
    df_sales["date"] = pd.to_datetime(df_sales["date"])
    df_sales["date"] = df_sales["date"].apply(lambda x: x.isoformat())

    with duckdb.connect(DB_EXPENSES) as conn:
        df_expenses = conn.execute("SELECT * FROM expenses").fetchdf()
    df_expenses["date"] = pd.to_datetime(df_expenses["date"])
    df_expenses["date"] = df_expenses["date"].apply(lambda x: x.isoformat())

    return df_sales, df_expenses


@task
def transform_data(df_sales: pd.DataFrame, df_expenses: pd.DataFrame):
    """Add revenue column to sales data and display in an artifact"""
    df_sales["revenue"] = round(df_sales["quantity"] * df_sales["price"])
    create_table_artifact(
        key="sales-report",
        table=df_sales.to_dict("records"),
        description="Sales report",
    )
    return df_sales, df_expenses


@task
def aggregate_financial_data(df_sales, df_expenses):
    """Aggregate financial data and display in an artifact"""
    date = df_sales["date"].iloc[0][:10]
    total_sales = df_sales["revenue"].sum().round()
    total_expenses = df_expenses["amount"].sum().round()
    summary_data = pd.DataFrame(
        {
            "Date": [date],
            "Total Sales": [total_sales],
            "Total Expenses": [total_expenses],
            "Net Income": [total_sales - total_expenses],
        }
    )

    create_table_artifact(
        key="financial-report",
        table=summary_data.to_dict("records"),
        description="Financial report",
    )
    return summary_data.iloc[0]


@task
def load_data(summary_data: pd.Series):
    """Load aggregated data into financial report database"""
    with duckdb.connect(DB_FINANCIAL) as conn:
        conn.execute(
            """
            INSERT INTO financial_report (date, total_sales, total_expenses, net_income)
            VALUES (?, ?, ?, ?)
            """,
            (
                summary_data["Date"],
                summary_data["Total Sales"],
                summary_data["Total Expenses"],
                summary_data["Net Income"],
            ),
        )
    print(f"Loaded record into financial report database")


@load_data.on_rollback
def rollback_financial_report(transaction):
    """Delete row on failure"""
    # with duckdb.connect(DB_FINANCIAL) as conn:
    #     conn.execute(
    #         """
    #         DELETE FROM financial_report
    #         WHERE date = ?
    #         """,
    #         (date,),
    #     )
    # could add a HITL check earlier
    print("Rolled back financial report due to failure")


@flow()
def financial_reporting_etl():
    """ETL pipeline for financial reporting"""
    with transaction():
        df_sales, df_expenses = extract_data()
        df_sales, df_expenses = transform_data(df_sales, df_expenses)
        aggregated_data = aggregate_financial_data(df_sales, df_expenses)
        load_data(aggregated_data)

    with duckdb.connect(DB_FINANCIAL) as conn:
        df = conn.execute("SELECT * FROM financial_report").fetchdf()
        print(df)


if __name__ == "__main__":
    financial_reporting_etl()
