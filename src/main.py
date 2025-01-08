from datetime import datetime
from pathlib import Path
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import io
from urllib.parse import urlparse
import csv

from jinja2 import Environment, FileSystemLoader
import boto3, botocore


def generate_message(row):
    if row["display_name"] == "":
        display_name = row["person_id"].split(".")[0]
    else:
        display_name = row["display_name"]

    return f"Happy {row['type'].lower()}, {display_name}! \U0001f389 \nI hope you have a great day! \U0001f600"


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
    buffer = io.BytesIO()
    if in_prod_env:
        parsed_url = urlparse(s3_file_path)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip("/")
        logger.info(f"Attempting to use file '{object_key}' in bucket '{bucket_name}'")

        try:
            s3 = boto3.client("s3")
            s3.download_fileobj(bucket_name, object_key, buffer)
            buffer.seek(0)
        except botocore.exceptions.ClientError:
            logger.error(f"Failed to download file '{object_key}'")
            raise
    else:
        local_file_path = Path(__file__).parents[1] / "data/dates.csv"
        with open(local_file_path, "rb") as f:
            buffer.write(f.read())
            buffer.seek(0)

    reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8"))
    today_events = []  # list of dictionaries
    for row in reader:
        # capture today's events
        row["date"] = datetime.strptime(row["date"], "%Y-%m-%d")
        if row["date"].month == today.month and row["date"].day == today.day:
            row["message"] = generate_message(row)
            today_events.append(row)
    today_events.sort(key=lambda x: (x["type"], x["date"]))

    # generate email content
    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template_txt = env.get_template("template.txt")
    template_html = env.get_template("template.html")

    output_txt = template_txt.render(reminders=today_events)
    output_html = template_html.render(reminders=today_events)

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
