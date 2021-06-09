import json
import os

from knowledge_graph_etl.exporter.database.database import Database
from knowledge_graph_etl.services.update_date_token_credit_score import update_token_credit_score


class CreditScoreService:
    BNB = "0x"

    def __init__(self, database=Database(), token_info="./infoToken.json"):
        self.database = database
        self.fix_prices = {
            "0x": {
                "price": 343.30,  ### fix price for BNB
                "decimals": 18
            }
        }
        cur_path = os.path.dirname(os.path.realpath(__file__))
        path_market = cur_path + "/" + token_info
        with open(path_market, "r") as file:
            self.tokens_market = json.load(file)

        self.balance_threshold = 1000
        self.supply_threshold = 1000

    def get_credit_score(self, wallet_address):
        try:
            wallet = self.database.get_wallet(wallet_address)
            ### 1.Số dư ví hiện tại
            x1 = self._get_score_balance(wallet)
            a1 = 0.3

            ### 2. Tiền cho vay/gửi tiết kiệm
            x2 = self._get_score_supply(wallet)
            a2 = 0.2
            ### x3: Lịch sử tín dụng (3=0.4)
            a3 = 0.4
            b31 = 0.3
            b32 = 0.3
            b33 = 0.4
            x31 = self._get_outstanding_ratio(wallet)

            x32 = self._score_accumulate_repay_on_borrow(wallet)

            x33 = self._score_time_liquidate_on_borrow(wallet)

            ### digital asset: 4=0.1
            x4 = self._get_max_token_score(wallet)

            credit_score = a1 * x1 + a2 * x2 + (a3 / 4) * (b31 * x31 + b32 * x32 + b33 * x33) + 0.1 * x4
            return credit_score
        except Exception as e:
            print(e)
            return 0

    ### 1.Số dư ví hiện tại
    def _get_score_balance(self, wallet):
        try:
            total_balance = self._get_total_balance(wallet)
            if total_balance > self.balance_threshold:
                return 1000
            else:
                return 0
        except Exception as e:
            print(e)
            return 0

    ### Tiền cho vay/gửi tiết kiệm
    def _get_score_supply(self, wallet):
        try:
            total_supply = self._get_total_supply(wallet)
            if total_supply > self.supply_threshold:
                return 1000
            else:
                return 0

        except Exception as e:
            print(e)
            return 0

    ### x31:  Tỷ lệ số dư/ nợ hiện tại (31=0.3)
    def _get_outstanding_ratio(self, wallet):
        try:
            total_balance = int(self._get_total_balance(wallet))
            total_borrow = int(self._get_total_borrow(wallet))
            if total_borrow == 0:
                return 1000
            return min(1, total_balance / total_borrow) * 1000



        except Exception as e:
            print(e)
            return 0

    ###x32: Số tiền đã trả/tổng nợ (32=0.3)
    def _score_accumulate_repay_on_borrow(self, wallet):
        try:
            accumulate_repay = self._get_accumulate_repay(wallet)
            accumulate_borrow = self._get_accumulate_borrow(wallet)

            if accumulate_borrow == 0:
                return 0
            return accumulate_repay * 1000 / accumulate_borrow
        except Exception as e:
            print(e)
            return 0

    ### x33: Các lần bị thanh toán khoản nợ (34=0.4)
    def _score_time_liquidate_on_borrow(self, wallet):
        try:
            borrow_time = self._i_get_accumulate_time(wallet, "Borrow")
            liquidate_time = self._i_get_accumulate_time(wallet, "LiquidateBorrow-borrower")

            if borrow_time == 0:
                return 0

            return (1 - liquidate_time / borrow_time) * 1000

        except Exception as e:
            print(e)
            return 0

    ### 4. digital asset: 4=0.1
    def _get_max_token_score(self, wallet):
        if not wallet.get("balances"):
            return 0
        balances = wallet.get("balances")
        token_score_wallet = 0
        for token_address in balances:
            if not self.tokens_market.get(token_address):
                continue

            token_score = self.tokens_market.get(token_address).get("credit_score")
            # print(token_score)
            token_score_wallet = max(token_score_wallet, token_score)

        return token_score_wallet

    def _get_total_balance(self, wallet):
        try:
            if wallet.get("balances"):
                balances = wallet.get("balances")
                sum_balance = 0
                for token_address in balances:
                    token_address = token_address.lower()
                    balance = balances.get(token_address)
                    if not balance:
                        continue
                    if token_address == CreditScoreService.BNB:

                        sum_balance += int(balance) * \
                                       self.fix_prices[token_address].get("price") / \
                                       10 ** int(self.fix_prices[token_address].get("decimals"))
                    else:

                        token = self.tokens_market.get(token_address)
                        if not token:
                            continue
                        decimals = int(token.get("decimals"))
                        if not decimals:
                            continue
                        price = float(token.get("price"))
                        if not price:
                            continue

                        sum_balance += int(balance) * price / 10 ** decimals
                return sum_balance

            else:
                return 0
        except Exception as e:
            print(e)
            return 0

    def _get_total_supply(self, wallet):
        total = self._i_get_total_supply_or_borrow(wallet, "supply")
        return total

    def _get_total_borrow(self, wallet):
        total = self._i_get_total_supply_or_borrow(wallet, "borrow")
        return total

    def _i_get_total_supply_or_borrow(self, wallet, typ="supply"):
        try:
            lending_info = wallet.get("lending_info")
            if not lending_info:
                return 0
            total_supply = 0
            for token_address in lending_info:
                supply_token = lending_info.get(token_address).get(typ)
                if not supply_token:
                    continue
                token = self.tokens_market.get(token_address)
                if not token:
                    continue
                decimals = int(token.get("decimals"))
                price = float(token.get("price"))

                total_supply += int(supply_token) * price / 10 ** decimals

            return total_supply
        except Exception as e:
            print(e)
            return 0

    def _get_accumulate_borrow(self, wallet):
        total = self._i_get_accumulate(wallet)
        return total

    def _get_accumulate_repay(self, wallet):
        total = self._i_get_accumulate(wallet, "RepayBorrow")
        return total

    def _i_get_accumulate(self, wallet, typ="Borrow"):
        try:
            accumulate = wallet.get("accumulate")
            if not accumulate:
                return 0
            total_supply = 0
            activity = accumulate.get(typ)
            if not activity:
                return 0

            for token_address in activity:
                accumulate_amount = activity.get(token_address).get("accumulate_amount")
                if not accumulate_amount:
                    continue
                token = self.tokens_market.get(token_address)
                if not token:
                    continue
                decimals = int(token.get("decimals"))
                price = float(token.get("price"))

                total_supply += int(accumulate_amount) * price / 10 ** decimals

            return total_supply
        except Exception as e:
            print(e)
            return 0

    def _i_get_accumulate_time(self, wallet, typ="Borrow"):
        try:
            accumulate_history = wallet.get("accumulate_history")
            if not accumulate_history:
                return 0
            total_time = 0
            activity = accumulate_history.get(typ)
            if not activity:
                return 0

            for token_address in activity:
                accumulate_at_token = activity.get(token_address)
                total_time = total_time + len(accumulate_at_token)

            return total_time
        except Exception as e:
            print(e)
            return 0

    def update_token_market_info(self):
        update_token_credit_score(self.database)
        return 0

    def get_token_market_info(self):
        return {}

    def test_print(self, address):
        wallet = self.database.get_wallet(address)

        print(self._get_max_token_score(wallet))


"""
keep acc 
0x00b23015762b310421e8f940b97f4180d084dda1
0xe2477627fa2db8ba2a4fe467876023987c3a7e8e
0xe2477627fa2db8ba2a4fe467876023987c3a7e8e

0x956bce4f086dc4579b960ed80336ef79737cdaa3
0x48620b6a00ff75d17082c81bd97896517332c6fe

781629306315660
663171985091646
"""
# credi = CreditScoreService()
# score = credi.test_print("0x0d0707963952f2fba59dd06f2b425ace40b492fe")
# print("score")
# print(score)


class TokenMarketInfo:
    name = ""
    address = ""
    symbol = ""
    price_coin = 0
    market_rank = 0
    highest_price = 0
    market_cap = 0
