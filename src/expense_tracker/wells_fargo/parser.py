import re
from typing import Literal

import camelot.io
import pandas as pd
import tabula.io

from expense_tracker.utils.logger import LOGGER
from expense_tracker.utils.parser import CSVParser, PDFParser
from expense_tracker.utils.text_ops import random_id


class WellsFargoParser(PDFParser):
    def __init__(self, pdf_name: str, bank: Literal["WellsFargo"] = "WellsFargo"):
        super().__init__(pdf_name, bank)
        self.id = self.pdf_name.split(" ")[0]  # Extract ID from the filename
        self.year = self.id[-2:]  # Extract year from the ID
        self.date_format = "%m/%d/%Y"  # Define the date format
        self.columns = [
            "Card",
            "Date",
            "Date2",
            "ID",
            "Description",
            "Credit",
            "Amount",
        ]

    def extract_tables(self):
        """
        Extract tables from the PDF using tabula.
        This method is called by the parse method.
        """
        self.tables = camelot.io.read_pdf(
            self.pdf_path,
            pages="all",
            flavor="stream",
        )
        return self.tables

    def parse(self):
        tables = self.extract_tables()
        if tables:
            i = 0
            while i < len(tables) and not tables[i]:
                try:
                    df: pd.DataFrame = tables[2].df
                    # Set the column names
                    try:
                        df.columns = self.columns
                        df.drop(columns=["Date2"], inplace=True)
                    except ValueError:
                        LOGGER.error(
                            "Error setting DataFrame columns. Check the PDF structure."
                        )
                        self.columns.remove("Date2")
                        df.columns = self.columns
                except ValueError:
                    continue
                i += 1
            if i >= len(tables):
                LOGGER.error("No valid tables found in the PDF.")
                return pd.DataFrame(columns=self.columns)
            # Clean the Date column
            df = df[
                (df["Date"].str.strip() != "") | (df["Date"].str.match(r"^[0-9\W]+$"))
            ]
            df.dropna(subset=["Date"], inplace=True)
            # Add year to the date
            df["Date"] += f"/20{self.year}"
            df["Date"] = pd.to_datetime(df["Date"], format=self.date_format)
            # Convert numeric columns to float
            df["Credit"] = df["Credit"].apply(
                lambda x: float(x) if x.strip() != "" else 0.0
            )
            df["Amount"] = df["Amount"].apply(
                lambda x: float(x) if x.strip() != "" else 0.0
            )
            df["Amount"] = df["Amount"] + df["Credit"]
            df.drop(columns=["Credit"], inplace=True)
            # Reset index, set df attribute and return the DataFrame
            df.reset_index(drop=True, inplace=True)
            self.df = df
            return df.reset_index(drop=True)
        LOGGER.warning("No tables found in the PDF.")
        self.df = pd.DataFrame(columns=self.columns)
        return self.df

    def save_to_aggregate(self):
        """
        Save the parsed DataFrame to the aggregate file.
        If the file does not exist, it will be created.
        If it exists, new data will be appended, and duplicates will be removed.
        """
        if hasattr(self, "df") and not self.df.empty:
            if self.aggregate_file.exists():
                temp_df = pd.read_csv(
                    self.aggregate_file, sep="\t", parse_dates=["Date"]
                )
                if not temp_df.empty:
                    self.df = pd.concat([temp_df, self.df], ignore_index=True)
                self.df.drop_duplicates(subset=["Date", "Description"], inplace=True)
            self.df.sort_values(by=["Date", "Description"], inplace=True)
            self.df.to_csv(self.aggregate_file, sep="\t", index=False)
            LOGGER.info(f"Data saved to {self.aggregate_file}")
        else:
            LOGGER.warning("No data to save.")


class WellsFargoYearEndSummaryParser(CSVParser):
    def __init__(
        self,
        csv_name: str,
        bank: Literal["WellsFargo", "Chase", "CapitalOne"] = "WellsFargo",
        data_type: Literal["YearEnd", "Statements", "AccountActivity"] = "YearEnd",
    ):
        super().__init__(csv_name, bank, data_type)
        self.card_id = 0000

    def load_df(self):
        """
        Load the CSV file into a DataFrame.
        """
        super().load_df()
        if self.df.empty:
            LOGGER.warning(f"No data found in {self.csv_name}.")
        else:
            self.df.drop(columns=["Unnamed: 8"], inplace=True, errors="ignore")
            self.df["Payment Method"] = self.df["Payment Method"].apply(
                lambda x: (
                    x.strip()[-4:] if isinstance(x, str) and len(x.strip()) > 4 else x
                )
            )
            self.df.rename(
                columns={
                    "Payment Method": "Card",
                },
                inplace=True,
            )
            self.create_id()
            self.card_id = self.df["Card"].iloc[0] if not self.df.empty else 0000
            self.df["Amount"] = self.df["Amount"] * -1  # Make all amounts positive
            self.df["Bank"] = self.bank
        return self.df

    def save_to_aggregate(self):
        """
        Save the parsed DataFrame to the aggregate file.
        If the file does not exist, it will be created.
        If it exists, new data will be appended, and duplicates will be removed.
        """
        file = self.aggregate_file
        if hasattr(self, "df") and not self.df.empty:
            if file.exists():
                temp_df = pd.read_csv(file, sep="\t", parse_dates=["Date"])
                max_date = temp_df.loc[temp_df["Card"] == self.card_id, "Date"].max()
                if max_date is pd.NaT:
                    max_date = pd.Timestamp.min
            else:
                temp_df = pd.DataFrame(columns=self.df.columns)
                max_date = pd.Timestamp.min
            # Only add new data if add_new_only is True
            if self.add_new_only == True:
                self.df = self.df[self.df["Date"] > max_date]
            # Remove payments, keep only transactions
            self.df = self.df[
                self.df["Description"].str.contains(
                    r"\b(payment|thank you)\b", regex=True, case=False
                )
                == False
            ]
            # Make all amounts positive
            self.df = pd.concat([temp_df, self.df], ignore_index=True)
            self.df.drop_duplicates(subset=["Date", "Description"], inplace=True)
            self.df.sort_values(by=["Date", "Description"], inplace=True)
            self.df.to_csv(file, sep="\t", index=False)
            LOGGER.info(f"Data saved to {file}")
        else:
            LOGGER.warning("No data to save.")

    def create_id(self):
        """
        Create a unique ID for the year-end summary.
        This can be based on the CSV name or other criteria.
        """
        self.df["ID"] = [random_id() for _ in range(len(self.df))]

    def save_to_global_aggregate(self):
        """
        Save the parsed DataFrame to the global aggregate file.
        If the file does not exist, it will be created.
        If it exists, new data will be appended, and duplicates will be removed.
        """
        local_data = pd.read_csv(self.aggregate_file, sep="\t", parse_dates=["Date"])
        if self.global_aggregate_file.exists():
            global_data = pd.read_csv(
                self.global_aggregate_file, sep="\t", parse_dates=["Date"]
            )
            data = pd.concat([global_data, local_data], ignore_index=True)
        else:
            data = local_data
        data.drop_duplicates(subset=["Date", "Description", "ID"], inplace=True)
        data.sort_values(by=["Date", "Description"], inplace=True)
        data.to_csv(self.global_aggregate_file, sep="\t", index=False)


class WellsFargoAccountSummaryParser(WellsFargoYearEndSummaryParser):
    def __init__(
        self,
        csv_name: str,
        bank: Literal["WellsFargo", "Chase", "CapitalOne"] = "WellsFargo",
        data_type: Literal["AccountActivity"] = "AccountActivity",
    ):
        super().__init__(csv_name, bank, data_type)
        self.columns = ["Date", "Amount", "0", "1", "Description"]
        self.get_card_id()

    def get_card_id(self):
        """
        Extract the card ID from the CSV name.
        This is a placeholder method and should be implemented based on actual logic.
        """
        LOGGER.debug(f"Processing CSV: {self.csv_name}")
        match = re.search(r"journey", self.csv_name, re.IGNORECASE)
        LOGGER.debug(f"Match found: {match}")
        if match:
            self.card_id = 9992
        else:
            self.card_id = 4031
        LOGGER.debug(f"Card ID set to: {self.card_id}")

    def load_df(self):
        """
        Load the CSV file into a DataFrame.
        """
        self.df = pd.read_csv(
            self.csv_file_path, names=self.columns, parse_dates=["Date"]
        )
        if self.df.empty:
            LOGGER.warning(f"No data found in {self.csv_name}.")
        else:
            self.df.drop(columns=["0", "1"], inplace=True, errors="ignore")
            self.create_id()
            self.df["Card"] = self.card_id
            self.df.sort_values(by=["Date", "Description"], inplace=True)
            self.card_id = self.df["Card"].iloc[0] if not self.df.empty else 0000
            self.df["Amount"] = self.df["Amount"] * -1  # Make all amounts positive
        return self.df


if __name__ == "__main__":
    # Example usage of WellsFargoAccountSummaryParser
    parser = WellsFargoAccountSummaryParser(csv_name="11Jun2025-23July2025 Journey.csv")
    df = parser.load_df()
    print(df.head())
    parser.save_to_aggregate()
