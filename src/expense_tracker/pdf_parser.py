from pathlib import Path
import pandas as pd
from typing import Literal
from tabula.io import read_pdf
from abc import ABC, abstractmethod

import datetime
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

data_path = Path.cwd() / "data"


class PDFParser(ABC):
    def __init__(
        self,
        pdf_name: str,
        bank: Literal[
            "WellsFargo", "Chase", "CapitalOne", "Ally", "Deserve", "Syncrony"
        ] = "WellsFargo",
    ):
        self.df = pd.DataFrame()
        self.tables = []
        self.id = None
        self.year = None
        self.bank = bank
        self.pdf_name = pdf_name
        self.pdf_path = data_path / self.bank / pdf_name
        self.aggregate_file = data_path / self.bank / "aggregate.tsv"

    def extract_tables(self):
        # Extract tables from the PDF
        tables = read_pdf(
            self.pdf_path,
            pages="all",
            multiple_tables=True,
            pandas_options={"header": None},
        )
        self.tables = tables
        return tables

    @abstractmethod
    def parse(self):
        """
        Abstract method to parse the PDF and return a DataFrame.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def save_to_aggregate(self):
        """
        Save the parsed DataFrame to the aggregate file.
        If the file does not exist, it will be created.
        """
        pass

    def save_to_tsv(self, filename: str | None = None):
        """
        Save the parsed DataFrame to a TSV file.
        """
        if filename is None:
            filename = str(self.pdf_path).split(".")[0] + ".tsv"
        if hasattr(self, "df") and not self.df.empty:
            self.df.to_csv(filename, header=True, index=False, sep="\t")
            LOGGER.info(f"Data saved to {filename}")
        else:
            LOGGER.warning("No data to save.")


class WellsFargoParser(PDFParser):
    def __init__(self, pdf_name: str, bank: Literal["WellsFargo"] = "WellsFargo"):
        super().__init__(pdf_name, bank)
        self.id = self.pdf_name.split(" ")[0]  # Extract ID from the filename
        self.year = self.id[-2:]  # Extract year from the ID
        self.date_format = "%m/%d/%Y"  # Define the date format

    def parse(self):
        tables = self.extract_tables()
        if tables:
            # Assuming the last table is the one we want
            df = tables[-1]  # type: ignore
            # Set the column names
            df.columns = [
                "card",
                "Date",
                "Date2",
                "ID",
                "Description",
                "Empty",
                "Amount",
            ]
            df.drop(columns=["Date2", "Empty"], inplace=True)
            df["Date"] += f"/20{self.year}"  # Add year to the date
            df["Date"] = pd.to_datetime(df["Date"], format=self.date_format)
            df.reset_index(drop=True, inplace=True)
            self.df = df
            return df.reset_index(drop=True)
        LOGGER.warning("No tables found in the PDF.")
        self.df = pd.DataFrame()
        return pd.DataFrame()

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
                self.df.drop_duplicates(inplace=True)
            self.df.sort_values(by=["Date", "Description"], inplace=True)
            self.df.to_csv(self.aggregate_file, sep="\t", index=False)
            LOGGER.info(f"Data saved to {self.aggregate_file}")
        else:
            LOGGER.warning("No data to save.")


if __name__ == "__main__":
    # Example usage
    parser = WellsFargoParser(pdf_name="112224 WellsFargo.pdf", bank="WellsFargo")
    transactions = parser.parse()
    if not transactions.empty:
        print(transactions.head())
    else:
        print("No transactions found.")
