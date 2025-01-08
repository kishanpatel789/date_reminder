from datetime import datetime
from pathlib import Path
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import io
from urllib.parse import urlparse

import pandas as pd
from jinja2 import Environment, FileSystemLoader
import boto3, botocore


def generate_message(row):
    if pd.isna(row["display_name"]):
        display_name = row["person_id"].split(".")[0]
    else:
        display_name = row["display_name"]

    return f"Happy {row['type']}, {display_name}! \U0001f389 \nI hope you have a great day! \U0001f600"


def main():

    # read environment vars
    sender = os.environ["DR_SENDER"]
    recipient = os.environ["DR_RECIPIENT"]
    in_prod_env = bool(int(os.environ.get("DR_PROD", "0")))
    if in_prod_env:
        s3_file_path = os.environ["DR_S3_PATH"]

    # get current date
    today = datetime.today()

    # load data file
    if in_prod_env:
        parsed_url = urlparse(s3_file_path)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip("/")
        logger.info(f"Attempting to use file '{object_key}' in bucket '{bucket_name}'")

        try:
            buffer = io.BytesIO()
            s3 = boto3.client("s3")
            s3.download_fileobj(bucket_name, object_key, buffer)
            buffer.seek(0)
        except botocore.exceptions.ClientError:
            logger.error(f"Failed to download file '{object_key}'")
            raise

        data_source = buffer
    else:
        data_source = Path(__file__).parents[1] / "data/dates.csv"

    df = pd.read_csv(
        data_source,
        dtype={"display_name": pd.StringDtype(storage="pyarrow")},
        parse_dates=["date"],
    )

    # filter data for current month-day
    mask = (df["date"].dt.month == today.month) & (df["date"].dt.day == today.day)
    df_filtered = df[mask].copy()

    # if records exist, process them
    if df_filtered.empty:
        logger.info("No matching records found")
        return

    # generate message
    df_filtered["message"] = df.apply(generate_message, axis=1)
    df_filtered.sort_values(["type", "date"], inplace=True)

    # generate email content
    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template_txt = env.get_template("template.txt")
    template_html = env.get_template("template.html")

    output_txt = template_txt.render(reminders=df_filtered)
    output_html = template_html.render(reminders=df_filtered)

    if not in_prod_env:
        output_path = Path(__file__).parents[1] / "out"
        with open(output_path / "out.txt", "w") as f:
            f.write(output_txt)
        with open(output_path / "out.html", "w") as f:
            f.write(output_html)

    logger.debug(output_txt)

    # generate email
    subject = f"Date Reminder - {today.strftime('%m/%d')}"

    message = MIMEMultipart("alternative")
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = subject

    message.attach(MIMEText(output_txt, "plain"))
    message.attach(MIMEText(output_html, "html"))

    # send email
    ses = boto3.client("sesv2")
    response = ses.send_email(Content={"Raw": {"Data": message.as_bytes()}})

    logger.info(response)

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Email sent successfully. MessageId is: {response['MessageId']}"
        ),
    }


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
