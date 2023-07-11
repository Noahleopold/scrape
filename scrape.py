from pydantic import BaseModel
from bs4 import BeautifulSoup
import requests
from typing import Optional
import re
import pandas as pd
import time
import json
from sqlalchemy import create_engine


import gspread
from google.oauth2.service_account import Credentials


from pandas import DataFrame

import os

CB_API_KEY = os.environ.get("CB_API_KEY")
if not CB_API_KEY:
    raise ValueError(
        "CB_KEY environment variable not set, should be a Crunchbase API key"
    )

DATABASE = os.environ.get("DATABASE")
if not DATABASE:
    raise ValueError(
        "DATABASE environment variable not set, should look like 'mysql+mysqlconnector://username:password@host:port/database'"
    )

engine = create_engine(DATABASE)  # type: ignore


def flatten_comany_name(company_name: str) -> str:
    # replace all whitespace with a single space
    company_name = re.sub(r"[.\s]+", "-", company_name)
    # leave only [a-zA-Z0-9_], then lowercase
    company_name = re.sub(r"[^a-zA-Z0-9_-]", "", company_name).lower()
    return company_name


class ScrapeProperties(BaseModel):
    name: str
    attrs: dict = {}


class CompanyRecord(BaseModel):
    firm: str
    company: str
    url: Optional[str] = None
    description: Optional[str] = None

    def to_CBCompanyRecord(self):
        self.url, self.description, self.crunchbase_url = self._get_crunchbase_data()

    def _get_crunchbase_data(self):
        # todo, this needs to hit a search endpoint instead of guessing the name
        # todo, maybe there is a batch endpoint as well?
        base_url = "https://api.crunchbase.com/api/v4/entities/organizations/"
        headers = {"X-Cb-User-Key": CB_API_KEY}
        formatted_company = re.sub(r"[ .]", "-", self.company.lower())
        url = f"{base_url}{formatted_company}?field_ids=short_description,website_url"
        response = requests.get(url, headers=headers)
        time.sleep(2)
        try:
            response.raise_for_status()
            data = response.json()
            short_description = data["properties"].get("short_description", "")
            website_url = data["properties"].get("website_url", "")
            crunchbase_url = (
                f"https://www.crunchbase.com/organization/{formatted_company}"
            )
            return website_url, short_description, crunchbase_url
        except (requests.exceptions.HTTPError, KeyError, json.JSONDecodeError) as e:
            print(f"Error retrieving data for {self.company}: {e}")
        return None, None, None


class CBCompanyRecord(CompanyRecord):
    crunchbase_url: Optional[str] = None
    crunchbase_description: Optional[str] = None
    crunchbase_website: Optional[str] = None

    @classmethod
    def from_company_record(cls, company_record: CompanyRecord):
        (
            cb_url,
            cb_description,
            cb_crunchbase_url,
        ) = company_record._get_crunchbase_data()
        return cls(
            crunchbase_url=cb_url,
            crunchbase_description=cb_description,
            crunchbase_website=cb_crunchbase_url,
            **company_record.model_dump(),
        )


class ScrapeTarget(BaseModel):
    name: str
    url: str
    target: ScrapeProperties
    name_target: Optional[ScrapeProperties] = None
    link_target: Optional[ScrapeProperties] = None
    description_target: Optional[ScrapeProperties] = None

    def scrape(self) -> list[CompanyRecord]:
        page_to_scrape = requests.get(str(self.url))
        soup = BeautifulSoup(page_to_scrape.text, "html.parser")
        hits = list(soup.findAll(**self.target.model_dump()))

        if self.name_target:
            name_hits = map(lambda c: c.find(**self.name_target.model_dump()), hits)
        else:
            name_hits = hits

        names = map(lambda c: flatten_comany_name(c.text) if c else None, name_hits)

        if self.link_target:
            link_hits = map(lambda c: c.find(**self.link_target.model_dump()), hits)
        else:
            link_hits = map(lambda c: c and c.find("a"), hits)

        links = map(lambda c: c.get("href") if c else None, link_hits)

        if self.description_target:
            description_hits = map(
                lambda c: c.find(**self.description_target.model_dump()), hits
            )
        else:
            description_hits = map(lambda c: c.find("p"), hits)

        descriptions = map(lambda c: c.text if c else None, description_hits)

        companies = [
            CompanyRecord(
                firm=flatten_comany_name(self.name),
                company=name,
                url=url,
                description=description,
            )
            for name, url, description in zip(names, links, descriptions)
            if name is not None
        ]

        return [CBCompanyRecord.from_company_record(c) for c in companies]


class Scrape(BaseModel):
    targets: list[ScrapeTarget]

    def __init__(self, targets: list[ScrapeTarget]) -> None:
        super().__init__(targets=targets)
        self.scrape()

    def scrape(self) -> list[CompanyRecord]:
        companies = []
        for target in self.targets:
            companies += target.scrape()
        return companies

    def data(self) -> pd.DataFrame:
        return pd.DataFrame.from_records([c.model_dump() for c in self.scrape()])

    def to_sql(self, table_name: str, engine=engine) -> None:
        data = self.data()
        data.to_sql(table_name, engine, if_exists="replace", index=False)


def database_to_sheets(
    db_table_name: str, sheet_name: str, sheet_table_name: str
) -> None:
    # Connect to the database
    query = f"SELECT * FROM {db_table_name}"
    df = pd.read_sql(query, con=engine)

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file("./scrape.json", scopes=scope)
    gc = gspread.authorize(creds)

    worksheet = gc.open(sheet_name).worksheet(sheet_table_name)
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


if __name__ == "__main__":
    scrape = Scrape(
        targets=[
            ScrapeTarget(
                name="Space Capital",
                url="https://www.spacecapital.com/portfolio",
                target=ScrapeProperties(
                    name="div",
                    attrs={"role": "listitem", "class": "collection-item-2 w-dyn-item"},
                ),
                name_target=ScrapeProperties(
                    name="div", attrs={"class": "portfolio-card-text bold-pfcard-text"}
                ),
                link_target=ScrapeProperties(
                    name="a", attrs={"class": "portfolio-linkblock2"}
                ),
                description_target=ScrapeProperties(
                    name="div", attrs={"class": "porfoliocard-textwrap"}
                ),
            )
        ]
    )
    print(scrape.data())
    scrape.to_sql("space_capital")
