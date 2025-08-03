from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from expense_tracker.main import fetch_data


def global_tab(tab, data: pd.DataFrame):
    with tab:
        st.subheader("Global Spending Overview")
        st.write("This section provides an overview of your spending across all banks.")

        # Create a bar chart of spending grouped by month
        # Aggregate spending by month using pandas PeriodIndex
        # Create a 'Month' column for grouping
        data["Month"] = pd.to_datetime(data["Date"]).dt.to_period("M").astype(str)
        monthly_spending = (
            data.groupby("Month")["Amount"]
            .sum()
            .reset_index()
            .rename(columns={"Amount": "Spending"})
        )

        st.subheader("Monthly Spending Bar Chart")
        chart = (
            alt.Chart(monthly_spending)
            .mark_bar()
            .encode(
                x=alt.X("Month", axis=alt.Axis(labelAngle=45)),
                y=alt.Y(
                    "Spending", axis=alt.Axis(format="$,.0f", title="Spending ($)")
                ),
            )
            .properties(width="container")
        )
        st.altair_chart(chart, use_container_width=True)
        st.write("Detailed Data:")
        st.dataframe(monthly_spending)
    return tab


def monthly_tab(data: pd.DataFrame, month: str, tab):
    month_data = data[
        pd.to_datetime(data["Date"]).dt.to_period("M").astype(str) == month
    ]
    with tab:
        st.subheader(f"Spending Overview for {month}")
        st.bar_chart(month_data.set_index("Date")["Amount"])
        st.write("Detailed Data:")
        st.dataframe(month_data.sort_values(by="Date", ascending=False))


def run():
    st.title("Monthly Spending Tracker")
    fetch_data()  # Ensure latest data is processed before loading
    data = pd.read_csv(
        Path("data") / "global_aggregate.tsv", sep="\t", parse_dates=["Date"], header=0
    )
    # Ensure 'Date' column only shows date (not time)
    data["Date"] = data["Date"].dt.date
    min_date = datetime(2025, 4, 1).date()
    data = data[data["Date"] >= min_date]
    # Get all months with data
    months_with_data = (
        pd.to_datetime(data["Date"]).dt.to_period("M").unique().astype(str)
    )
    print(f"Months with data: {months_with_data}")
    tabs = st.tabs(["Global Overview"] + months_with_data.tolist())
    global_tab(tabs[0], data)
    for month, tab in zip(months_with_data, tabs[1:]):
        monthly_tab(data, month, tab)


if __name__ == "__main__":
    run()
