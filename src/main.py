import pandas as pd
from datetime import datetime
from pathlib import Path
import logging


def main():

    # get current date
    today = datetime.today()

    # load data file
    df = pd.read_csv("data/dates.csv", parse_dates=["date"])
    logger.debug(df)

    # filter data for current month-day
    mask = (df["date"].dt.month == today.month) & (df["date"].dt.day == today.day)
    df_filtered = df[mask]

    # if records exist, process them
    if df_filtered.empty:
        logger.info("No matching records found")
    else:
        # generate message
        logger.debug(df_filtered)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    main()
