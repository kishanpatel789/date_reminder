import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

from jinja2 import Environment, FileSystemLoader



def generate_message(row):
    if pd.isna(row["display_name"]):
        display_name = row["person_id"].split(".")[0]
    else:
        display_name = row["display_name"]

    return f"Happy {row['type']}, {display_name}! I hope you have a great day!"


def main():

    # get current date
    today = datetime.today()

    # load data file
    df = pd.read_csv(
        "../data/dates.csv",
        dtype={"display_name": pd.StringDtype(storage="pyarrow")},
        parse_dates=["date"],
    )
    # logger.debug(df)
    # logger.debug(df.dtypes)

    # filter data for current month-day
    mask = (df["date"].dt.month == today.month) & (df["date"].dt.day == today.day)
    df_filtered = df[mask].copy()

    # if records exist, process them
    if df_filtered.empty:
        logger.info("No matching records found")
        return

    # generate message
    df_filtered["message"] = df.apply(generate_message, axis=1)
    df_filtered.sort_values("type", inplace=True)

    # generate email content
    env = Environment(
        loader=FileSystemLoader('./templates'),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template_txt = env.get_template('template.txt')
    template_html = env.get_template('template.html')

    output_txt = template_txt.render(reminders=df_filtered)


    logger.debug(output_txt)

    # 


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
