from datetime import date
import requests
import pandas as pd
import os
import sys
from bs4 import BeautifulSoup


COVID19_STAT_URL = "https://moz.gov.ua/koronavirus-2019-ncov"
BASE_EXCEL_DOC_URL = "https://moz.gov.ua"
SHEET_NAME = "Дані по лікарням"
BASE_PATH = os.path.expanduser("~")
MAIN_FILE_PATH = f"{BASE_PATH}\COVID19_SUPPLY_MOZ\covid19_supply_main_data.xlsx"
INCREMENT_PATH = f"{BASE_PATH}\COVID19_SUPPLY_MOZ\increment_history\covid19_supply_increment_{date.today()}.xlsx"


def get_link_for_fresh_doc(
    covid_stat_url: str, base_excel_doc_url: str, sheet_name: str
) -> str:
    """Method for retrieving the link for increment from the MOZ site"""

    html_text = requests.get(COVID19_STAT_URL).text
    soup = BeautifulSoup(html_text, "html.parser")
    # in case of issues with parser, look here:

    doc_url = (
        BASE_EXCEL_DOC_URL
        + soup.find_all("h5")[2].next_sibling.next_sibling.next_sibling.next_sibling.a[
            "href"
        ]
    )

    return doc_url


def read_the_increment(doc_url: str) -> pd.DataFrame:
    """Method which reads daily increment in Pandas DataFrame object"""
    print(f"Reading file from {doc_url}")
    return pd.read_excel(doc_url, sheet_name=SHEET_NAME)


def process_the_increment(df: pd.DataFrame) -> pd.DataFrame:
    """Method which process the increment DataFrame"""
    # Processing the express tests
    express_tests_df = df[df["Код показника"] == "rapid_tests_current"]
    express_tests_df = express_tests_df.rename(
        {
            "Unnamed: 6": "Поточний залишок (Швидкі)",
            "Unnamed: 8": "Використано за добу (Швидкі)",
        },
        axis="columns",
    )
    express_tests_df = express_tests_df[
        [
            "ЄДРПОУ",
            "Регіон",
            "Назва закладу",
            "Звітна дата",
            "Поточний залишок (Швидкі)",
            "Використано за добу (Швидкі)",
        ]
    ]

    # Processing the PCRs
    pcr_df = df[df["Код показника"] == "rcp_current"]
    pcr_df = pcr_df.rename(
        {
            "Unnamed: 6": "Поточний залишок (ПЛР)",
            "Unnamed: 8": "Використано за добу (ПЛР)",
        },
        axis="columns",
    )
    pcr_df = pcr_df[
        [
            "ЄДРПОУ",
            "Регіон",
            "Назва закладу",
            "Звітна дата",
            "Поточний залишок (ПЛР)",
            "Використано за добу (ПЛР)",
        ]
    ]

    # Processing the amplifs
    amplif_df = df[(df["Код показника"] == "delivery156")]
    amplif_df = amplif_df.rename(
        {"Unnamed: 6": "Поточний залишок (Ампліфікатори)"}, axis="columns"
    )
    amplif_df = amplif_df[
        [
            "ЄДРПОУ",
            "Регіон",
            "Назва закладу",
            "Звітна дата",
            "Поточний залишок (Ампліфікатори)",
        ]
    ]

    # Merging all three datasets together

    increment_df = express_tests_df.merge(
        pcr_df, how="outer", on=["ЄДРПОУ", "Регіон", "Назва закладу", "Звітна дата"]
    ).merge(
        amplif_df, how="outer", on=["ЄДРПОУ", "Регіон", "Назва закладу", "Звітна дата"]
    )

    # Save the increment for reliability purposes:

    save_df(save_path=INCREMENT_PATH, df=increment_df)

    return increment_df


def read_the_base_data() -> pd.DataFrame:
    try:
        base_df = pd.read_excel(MAIN_FILE_PATH)
    except FileNotFoundError:
        print(
            f"Main data file is absent. Please, insert it in {MAIN_FILE_PATH} with respect to main file name convention and restart the program"
        )
        input("Press Enter to close the window and finish execution")
        sys.exit()
    return base_df


def merge_increment(
    increment_df: pd.DataFrame, base_df: pd.DataFrame = None
) -> pd.DataFrame:
    """Method which merges old data (base_df) with the fresh increment in a SCD Type 1 style"""
    print("Reading the main data from Excel sheet")
    base_df = read_the_base_data()
    unionized_df = pd.concat([base_df, increment_df]).dropna()
    ordered_df = unionized_df.sort_values(by=["ЄДРПОУ", "Звітна дата"])
    print("Merging and deduplicating data from base and incremental sheets")
    deduplicated_df = ordered_df.drop_duplicates(subset=["ЄДРПОУ"], keep="last")

    return deduplicated_df


def save_df(save_path: str, df: pd.DataFrame) -> None:
    """Method for saving DataFrame in file system"""
    print(f"Saving data to path: {save_path}")
    df.to_excel(excel_writer=save_path, index=False)


def filesystem_path_utils() -> None:
    base_path = BASE_PATH
    if not os.path.exists(f"{base_path}\COVID19_SUPPLY_MOZ\\"):
        os.makedirs(f"{base_path}\COVID19_SUPPLY_MOZ\\")
        print(
            f"Created base directory for COVID19 supply data: {base_path}\COVID19_SUPPLY_MOZ\\"
        )
    if not os.path.exists(f"{base_path}\COVID19_SUPPLY_MOZ\\increment_history\\"):
        os.makedirs(f"{base_path}\COVID19_SUPPLY_MOZ\\increment_history\\")
        print(
            f"Created directory for COVID19 supply data increments history: {base_path}\COVID19_SUPPLY_MOZ\\increment_history\\"
        )
        print(
            "This program will fail now. Please, place base data in xlsx format with name 'covid19_supply_main_data' in directory {base_path}\COVID19_SUPPLY_MOZ\\"
        )


if __name__ == "__main__":
    filesystem_path_utils()
    save_df(
        save_path=MAIN_FILE_PATH,
        df=merge_increment(
            process_the_increment(
                read_the_increment(
                    get_link_for_fresh_doc(
                        covid_stat_url=COVID19_STAT_URL,
                        base_excel_doc_url=BASE_EXCEL_DOC_URL,
                        sheet_name=SHEET_NAME,
                    )
                )
            )
        ),
    )
    input("Press Enter to close the window and finish execution")
