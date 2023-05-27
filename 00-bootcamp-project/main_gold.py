import csv
import json
import scrapy
from google.cloud import bigquery
from google.oauth2 import service_account
from scrapy.crawler import CrawlerProcess


URL = "https://ทองคําราคา.com/"
DATA_FOLDER = "data"

# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "bigquery.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "kinetic-harbor-384413"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

def load_data_without_partition(data):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

   
    file_path = f"{DATA_FOLDER}/{data}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.deb_bootcamp.{data}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)
        with open(f"data/datagold.csv", "w") as f:
            writer = csv.writer(f)
            for row in rows:
                #print(row.css("td::text").extract())
          
                writer.writerow(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())
        load_data_without_partition("datagold")
        # Write to CSV
        # YOUR CODE HERE


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
