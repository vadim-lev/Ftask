import sys
import yaml
import json
from time import sleep
from datetime import datetime
from typing import Tuple
import logging
from logging import StreamHandler

import pandas as pd

from deribit_client import DeribitClient
from db_connection_tasks import get_postgresql_connection


def insert_to_db(schema_name: str, table_name: str, df: pd.DataFrame):

    conn = get_postgresql_connection(postgresql_secret_path="./secrets/postgresql/auth.json")
    transaction = conn.begin()
    logger.info(f"Insert data in table {schema_name}.{table_name}")
    count_rows = df.to_sql(name=table_name, schema=schema_name, con=conn, if_exists="append", index=False,
                           chunksize=1000)
    transaction.commit()
    conn.close()

    logger.info(f"Rows inserted  {schema_name}.{table_name} => {count_rows}")


def work_with_order(client, order: dict, current_price: float, gap: float, gap_ignore: float, row: dict) -> Tuple[dict, dict]:

    if order['order_type'] == "BUY_ORDER":

        if current_price <= order['order_price']:
            row['status_order'] = "Completed"
            order = client.create_sell_order(current_price=current_price, gap=gap, gap_ignore=gap_ignore)

        elif current_price > order['limit_price']:
            row['status_order'] = "Canceled"
            order = client.create_buy_order(current_price=current_price, gap=gap, gap_ignore=gap_ignore)

    elif order['order_type'] == "SELL_ORDER":

        if current_price >= order['order_price']:
            row['status_order'] = "Completed"
            order = client.create_buy_order(current_price=current_price, gap=gap, gap_ignore=gap_ignore)

        elif current_price < order['limit_price']:
            row['status_order'] = "Canceled"
            order = client.create_sell_order(current_price=current_price, gap=gap, gap_ignore=gap_ignore)

    return order, row


def work_robot(current_price: float):

    with open("./config.yaml") as config_file:
        config = yaml.safe_load(config_file)

    logger.debug(f"config: \n{json.dumps(config, indent=4)}")

    gap = config['robot']['gap']
    gap_ignore = config['robot']['gap_ignore']
    table = []

    instance_client = DeribitClient(client_config=config['exchange'])

    # create order
    order = instance_client.create_buy_order(current_price=current_price, gap=gap, gap_ignore=gap_ignore)

    # while True:
    for i in range(400):

        sleep(0.1)
        current_price = instance_client.get_price(order=order, current_price=current_price)

        row = {
            "time": datetime.now(),
            "order_type": order['order_type'],
            "size_order": 1,
            "buy_price_order": order['buy_price_order'],
            "order_price": order['order_price'],
            "limit_price": order['limit_price'],
            "current_price": current_price,
            "status_order": "Process"
        }

        order, row = work_with_order(client=instance_client, order=order, current_price=current_price, gap=gap,
                                     gap_ignore=gap_ignore, row=row)

        table.append(row)

    df_data = pd.DataFrame(
        data=table,
        columns=table[0].keys()
    )

    insert_to_db(schema_name="public", table_name="order_history", df=df_data)


if __name__ == "__main__":
    logger = logging.getLogger("my-logger")
    logger.setLevel(logging.DEBUG)
    handler = StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)

    work_robot(current_price=1000)

