from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import plotly.express as px
import streamlit as st

from expense_tracker.main import fetch_data


def format_amount_col(df: pd.DataFrame, col: str = "Amount") -> pd.DataFrame:
    """Format a specified column in the DataFrame to display as currency."""
    df[col] = df[col].astype(str)
    df[col] = df[col].map(lambda x: f"${float(x):,.2f}")
    return df


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
            .copy()
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
        st.dataframe(
            format_amount_col(monthly_spending, "Spending"),
            hide_index=True,
            use_container_width=True,
        )
    return tab


def monthly_tab(data: pd.DataFrame, month: str, tab):
    month_data = data[
        pd.to_datetime(data["Date"]).dt.to_period("M").astype(str) == month
    ].copy()
    with tab:
        cols = st.columns(2)
        st.subheader(f"Spending Overview for {month}")
        # Section with pie chart of spending by category for the month
        with cols[0]:
            fig = px.pie(
                month_data,
                names="Category",
                values="Amount",
                title=f"Spending Distribution for {month}",
                labels={"Amount": "Spending ($)"},
            )
            st.plotly_chart(fig, use_container_width=True)
        # Section with detailed spending by category
        with cols[1]:
            total_spending = (
                month_data.groupby("Category")["Amount"]
                .sum()
                .to_frame()
                .reset_index()
                .copy()
            )
            total_spending.loc[len(total_spending)] = [
                "Total",
                total_spending["Amount"].sum(),
            ]
            st.dataframe(
                format_amount_col(total_spending).rename(
                    columns={"Amount": "Spending"}
                ),
                hide_index=True,
                use_container_width=True,
            )
        st.write("Detailed Data:")
        monthly_data_display = format_amount_col(month_data).sort_values(
            by="Date", ascending=False
        )
        st.dataframe(
            monthly_data_display,
            hide_index=True,
            use_container_width=True,
        )


def run():
    st.set_page_config(
        page_title="Monthly Spending Tracker",
        page_icon=".streamlit/browser_logo.png",
    )
    # App header with logo next to title and introduction
    header_cols = st.columns([1, 5])
    with header_cols[0]:
        st.image(".streamlit/page_logo.png", width=80)
    with header_cols[1]:
        st.markdown(
            """
            # Monthly Spending Tracker
            """
        )
    st.markdown(
        "Welcome to your personal expense dashboard! "
        "This app helps you track and visualize your "
        "monthly spending across all your bank accounts. "
        "Use the tabs below to explore your global "
        "spending overview and drill down into individual "
        "months for detailed insights by category."
    )

    fetch_data()  # Ensure latest data is processed before loading
    data = pd.read_csv(
        Path("data") / "global_aggregate.tsv", sep="\t", parse_dates=["Date"], header=0
    )
    # Ensure 'Date' column only shows date (not time)
    data["Date"] = data["Date"].dt.date
    min_date = datetime(2025, 4, 1).date()
    data = data[data["Date"] >= min_date]
    data.fillna({"Category": "Uncategorized"}, inplace=True)
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
