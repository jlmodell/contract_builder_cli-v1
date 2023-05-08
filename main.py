from functools import reduce
from io import StringIO
from pathlib import Path
import os
import pandas as pd
from datetime import datetime, timedelta
import yaml
from rich import print
from pymongo import MongoClient
from pymongo.collection import Collection


BASE_PATH = Path(r"C:\temp")
CONFIG = os.path.join(BASE_PATH, "global", "config.yaml")
CONTRACT = os.path.join(BASE_PATH, "CONPRICE")
CLIENT = None
ATLAS_CLIENT = None

distributor_pct = 0
commission_pct = 4


def load_config():
    with open(CONFIG, "r") as f:
        config = yaml.safe_load(f)
        # print(config)
    return config


def set_percents() -> None:
    global distributor_pct, commission_pct
    distributor_pct = input(
        f"Enter distributor percentage: (default: {distributor_pct})\t > "
    )
    if distributor_pct == "":
        distributor_pct = 0
    else:
        distributor_pct = float(distributor_pct) / 100

    commission_pct = input(
        f"Enter commission percentage: (default: {commission_pct})\t > "
    )
    if commission_pct == "":
        commission_pct = 4 / 100
    else:
        commission_pct = float(commission_pct) / 100


set_percents()


def connect_clients(
    linode_uri: str,
    atlas_uri: str,
) -> tuple[MongoClient, MongoClient]:
    global CLIENT, ATLAS_CLIENT
    if CLIENT is None:
        CLIENT = MongoClient(linode_uri)
    if ATLAS_CLIENT is None:
        ATLAS_CLIENT = MongoClient(atlas_uri)

    return CLIENT, ATLAS_CLIENT


# wrapper to include client
def get_collection(client: MongoClient, db: str, collection: str) -> Collection:
    return client[db][collection]


def get_documents(collection: Collection, filter: dict, projection: dict) -> list:
    try:
        collection.find(
            filter=filter,
            projection=projection,
        )
    except Exception as e:
        raise e


def set_dates(
    now=datetime.now(),
) -> tuple[datetime, datetime, datetime, datetime]:
    start = datetime(now.year, now.month, 1) - timedelta(days=365)
    end = datetime(now.year, now.month, 1) - timedelta(days=1)

    return start, end


def combine_db_with_contract(c: dict, contract: dict) -> dict:
    client, atlas_client = connect_clients(
        c["mongodb"]["linode"]["uri"], c["mongodb"]["atlas"]["uri"]
    )
    collection = get_collection(client, "busse", "sales")
    cost_collection = get_collection(atlas_client, "bussepricing", "costs")

    ytd_start, ytd_end = set_dates()
    pytd_start, pytd_end = set_dates(now=(datetime.now() - timedelta(days=365)))

    for item in contract["items"].keys():
        cost_doc = cost_collection.find_one({"alias": item})

        if cost_doc:
            cost = cost_doc["cost"]
        else:
            cost = 0.0

        contract["items"][item]["cost"] = cost
        contract["items"][item]["distributor_fee"] = (
            distributor_pct * contract["items"][item]["price"]
        )
        contract["items"][item]["commission"] = (
            commission_pct * contract["items"][item]["price"]
        )
        contract["items"][item]["loaded_cost"] = (
            cost
            + contract["items"][item]["distributor_fee"]
            + contract["items"][item]["commission"]
        )
        contract["items"][item]["gross_profit"] = (
            contract["items"][item]["price"] - contract["items"][item]["loaded_cost"]
        )
        contract["items"][item]["gross_profit_pct"] = (
            contract["items"][item]["gross_profit"]
            / contract["items"][item]["price"]
            * 100
        )

        ytd_sales, ytd_qty = get_total_sales_and_qty(
            collection, ytd_start, ytd_end, cust=contract["customer_number"], item=item
        )
        contract["sales_history"]["ytd"][item]["qty"] = ytd_qty
        contract["sales_history"]["ytd"][item]["sales"] = ytd_sales

        pytd_sales, pytd_qty = get_total_sales_and_qty(
            collection,
            pytd_start,
            pytd_end,
            cust=contract["customer_number"],
            item=item,
        )
        contract["sales_history"]["pytd"][item]["qty"] = pytd_qty
        contract["sales_history"]["pytd"][item]["sales"] = pytd_sales

    return contract


def get_total_sales_and_qty(
    collection: Collection,
    start: datetime,
    end: datetime,
    cust: str = None,
    item: str = None,
    projection: dict = {
        "_id": 0,
        "SALE": 1,
        "QTY": 1,
    },
) -> tuple[float, float | int]:
    filter = {
        "DATE": {"$gte": start, "$lte": end},
    }

    if cust is not None:
        filter["CUST"] = cust
    if item is not None:
        filter["ITEM"] = item

    docs = list(collection.find(filter=filter, projection=projection))

    sales = (
        reduce(lambda x, y: x + y, [x["SALE"] for x in docs]) if len(docs) > 0 else 0.0
    )
    qty = reduce(lambda x, y: x + y, [x["QTY"] for x in docs]) if len(docs) > 0 else 0

    try:
        qty = int(qty)
    except Exception:
        qty = round(qty, 2)

    return sales, qty


def read_contract_file_from_roi():
    dtype = {
        0: "str",
        1: "str",
        2: "str",
        3: "str",
        4: "str",
        5: "str",
        6: "str",
        7: "str",
        8: "str",
        9: "str",
        10: "str",
        11: "str",
        12: "str",
        13: "int",
        14: "float",
        15: "str",
    }

    df = pd.read_csv(CONTRACT, header=None, dtype=dtype, low_memory=False)

    df.dropna(subset=[12], inplace=True)

    return df


def parse_contract_df(df: pd.DataFrame) -> dict:
    data = df.to_dict(orient="records")

    contract = {}

    contract["contract_number"] = data[0][0]
    contract["contract_name"] = data[0][2]
    contract["customer_number"] = data[0][1]
    contract["rep"] = data[0][3]
    contract["start_date"] = data[0][5]
    contract["end_date"] = data[0][6]
    contract["shipping_terms"] = data[0][7]
    contract["order_terms"] = data[0][8]
    contract["notes"] = data[0][10] + " " + data[0][5]

    contract["items"] = {}
    contract["sales_history"] = {}
    contract["sales_history"]["ytd"] = {}
    contract["sales_history"]["pytd"] = {}

    for item in data:
        contract["items"][item[12]] = {
            "item_number": item[12],
            "item_description": item[15],
            "price": item[14],
            "cost": 0.0,
            "distributor_fee": 0.0,
            "commission": 0.0,
            "loaded_cost": 0.0,
            "gross_profit": 0.0,
            "gross_profit_pct": 0.0,
            "uom": "CS",
        }
        contract["sales_history"]["ytd"][item[12]] = {
            "qty": 0,
            "sales": 0.0,
        }
        contract["sales_history"]["pytd"][item[12]] = {
            "qty": 0,
            "sales": 0.0,
        }

    return contract


def create_csv_from_contract(contract: dict) -> str:
    global distributor_pct, commission_pct

    csv_string = ""

    csv_string += "|".join(["", "", "", "", "", "", "", "", "", "", "Notes"]) + "\n"
    csv_string += (
        "|".join(["", "", "", "", "", "", "", "", "", "", contract["notes"]]) + "\n"
    )

    header = [
        "Contract",
        "Contract Name",
        "Customer",
        "Rep",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]

    csv_string += "|".join(header) + "\n"

    row = [
        contract["contract_number"],
        contract["contract_name"],
        contract["customer_number"],
        contract["rep"],
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]

    csv_string += "|".join(row) + "\n"

    csv_string += "|".join(["", "", "", "", "", "", "", "", "", "", ""]) + "\n"

    header = [
        "Start",
        "End",
        "Shipping Terms",
        "Order Terms",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]

    csv_string += "|".join(header) + "\n"

    row = [
        contract["start_date"],
        contract["end_date"],
        contract["shipping_terms"],
        contract["order_terms"],
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]

    csv_string += "|".join(row) + "\n"

    csv_string += "|".join(["", "", "", "", "", "", "", "", "", "", ""]) + "\n"

    pre_header = [
        "",
        "",
        "",
        "",
        "",
        f"{distributor_pct:.2f}%",
        f"{commission_pct:.2f}%",
        "",
        "",
        "",
        "",
    ]

    csv_string += "|".join(pre_header) + "\n"

    header = [
        "Item Number",
        "Item Description",
        "UOM",
        "Price",
        "Cost",
        "Distributor Fee",
        "Commission",
        "Loaded Cost",
        "Gross Profit",
        "Gross Profit %",
        "",
    ]

    csv_string += "|".join(header) + "\n"

    for item in contract["items"].keys():
        row = [
            item,
            contract["items"][item]["item_description"],
            contract["items"][item]["uom"],
            f'{contract["items"][item]["price"]:.2f}',
            f'{contract["items"][item]["cost"]:.2f}',
            f'{contract["items"][item]["distributor_fee"]:.2f}',
            f'{contract["items"][item]["commission"]:.2f}',
            f'{contract["items"][item]["loaded_cost"]:.2f}',
            f'{contract["items"][item]["gross_profit"]:.2f}',
            f'{contract["items"][item]["gross_profit_pct"]:.2f}',
            "",
        ]

        csv_string += "|".join([str(x) for x in row]) + "\n"

    csv_string += "|".join(["", "", "", "", "", "", "", "", "", "", ""]) + "\n"

    header = [
        "Item Number",
        "Item Description",
        "YTD Quantity",
        "YTD Sales",
        "",
        "PYTD Quantity",
        "PYTD Sales",
        "",
        "",
        "",
        "",
    ]

    csv_string += "|".join(header) + "\n"

    for item in contract["items"].keys():
        row = [
            item,
            contract["items"][item]["item_description"],
            contract["sales_history"]["ytd"][item]["qty"],
            f'{contract["sales_history"]["ytd"][item]["sales"]:.2f}',
            "",
            contract["sales_history"]["pytd"][item]["qty"],
            f'{contract["sales_history"]["pytd"][item]["sales"]:.2f}',
            "",
            "",
            "",
            "",
        ]

        csv_string += "|".join([str(x) for x in row]) + "\n"

    return csv_string


def main() -> tuple[dict, str]:
    c = load_config()

    df = read_contract_file_from_roi()

    initial_contract = parse_contract_df(df)
    contract = combine_db_with_contract(c, initial_contract)

    csv_string = create_csv_from_contract(contract)

    return contract, csv_string


if __name__ == "__main__":
    contract, csv_string = main()

    print(csv_string)

    csv_string_io = StringIO(csv_string)

    df = pd.read_csv(csv_string_io, header=None, delimiter="|")
    df.fillna("", inplace=True)

    df.to_excel(f"{contract['contract_number']}.xlsx", index=False, header=False)

    html = f"""
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Contract {contract["contract_number"]}</title>
        <style>
            * {{
                font-family: Arial, Helvetica, sans-serif;                
            }}
            table, th, td {{
                    border: none;
                    border-collapse: collapse;
                    padding: 5px;
            }}
        </style>
    </head>    
    <body>
    """

    df_html = df.to_html(index=False, header=False)

    html += df_html
    html += "</html>"

    with open(f"{contract['contract_number']}.html", "w") as f:
        f.write(html)

    print(df)
