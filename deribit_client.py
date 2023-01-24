import random
from typing import Dict, Any



class DeribitClient(object):
    def __init__(self, client_config: Dict[str, Any]):

        self.client_config = client_config

        # self.deribit_connect = Deribit.client(client_config)

    def create_buy_order(self, current_price: float, gap: float, gap_ignore: float):

        buy_price = current_price - gap / 2

        order = {
            "order_type": "BUY_ORDER",
            "buy_price_order": current_price,
            "order_price": buy_price,
            "limit_price": buy_price + gap + gap_ignore
        }

        # self.deribit_connect.create_order(order)

        # print(f"Order: {order}")
        # print(f"___________________________CREATE BUY ORDER___________________________")

        return order

    def create_sell_order(self, current_price: float, gap: float, gap_ignore: float):

        sell_price = current_price + gap

        order = {
            "order_type": "SELL_ORDER",
            "buy_price_order": current_price,
            "order_price": sell_price,
            "limit_price": sell_price - gap - gap_ignore
        }

        # self.deribit_connect.create_order(order)

        # print(f"___________________________CREATE SELL ORDER___________________________")
        # print(f"Order: {order}")

        return order

    def get_price(self, order: dict, current_price: float) -> float:

        # self.deribit_connect.get_current_price(order)

        next_price = round(random.normalvariate(current_price, current_price / 20), 4)
        # print(f"current_price = {next_price}")

        return next_price

    def cancel_order(self, order):

        # self.deribit_connect.cancel_order(order)
        print(f"Cancel {order['order_type']}.")


    def close_order(self, order):

        # self.deribit_connect.close_order(order)
        print(f"{order['order_type']} completed.")

