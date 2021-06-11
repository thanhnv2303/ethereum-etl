import json
import os

import numpy as np
from pycoingecko import CoinGeckoAPI

from knowledge_graph_etl.exporter.database.database import Database


def update_token_credit_score(fileInput='listToken.txt', fileOutput='infoToken.json', database=Database()):
    cg = CoinGeckoAPI()
    # Const
    currency = 'usd'
    #############
    # Get list of coins
    cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../"
    fileInput = cur_path + fileInput
    fileOutput = cur_path + fileOutput
    #############
    # Get list of coins
    f = open(fileInput, "r")
    # Init
    coin = list()
    addr = list()
    id = list()

    for x in f:
        data = x.split(" ")
        coin.append(data[0])
        addr.append(data[1])
        data[2] = data[2].replace('\n', '')
        id.append(data[2])
    # Init Array
    priceCoin = np.zeros(len(id))
    marketRank = np.zeros(len(id))
    highestPrice = np.zeros(len(id))
    creditScore = np.zeros(len(id))
    market_cap = np.zeros(len(id))
    num = 0
    for i in range(len(id)):
        try:
            if (id[i] == 'bitcoin-bep2'):  # if coin = bnb bitcoin, get info of bitcoin
                data = cg.get_coin_by_id(id='bitcoin', vs_currencies=currency)
            else:
                data = cg.get_coin_by_id(id=id[i], vs_currencies=currency)

            # print(cg.get_coin_info_from_contract_address_by_id(contract_address=addr[i],id = 56 ))
            priceCoin[i] = data['market_data']['current_price'][currency]
            marketRank[i] = data['market_cap_rank']
            highestPrice[i] = data['market_data']['ath'][currency]
            market_cap[i] = data['market_data']['market_cap'][currency]
            num = num + 1
        except Exception as e:
            print(e)
    ###############
    # Calculate Credit Score

    marketRank[np.isnan(marketRank)] = 0
    np.nan_to_num(marketRank)
    for i in range(num):
        if marketRank[i] > 1000 or marketRank[i] == 0:
            rank = 0
        else:
            rank = 1001 - marketRank[i]
        ratioPrice = priceCoin[i] / highestPrice[i]
        creditScore[i] = int(0.6 * rank + 400 * ratioPrice)
    priceStr = ["%.2f" % j for j in priceCoin]

    with open(fileOutput, 'w') as f:
        tokens_dict = {}
        for i in range(len(id)):
            addr[i] = str(addr[i].lower())
            line = addr[i] + ' ' + coin[i] + ' ' + priceStr[i] + ' ' + str(creditScore[i]) + ' ' + str(
                market_cap[i])
            print(line)
            tokens_dict[addr[i]] = {
                "symbol": coin[i],
                "price": priceStr[i],
                "credit_score": creditScore[i],
                "market_cap": market_cap[i]
            }

            token = database.get_token(addr[i].lower())
            if token:
                token["price"] = priceStr[i]
                token["credit_score"] = str(creditScore[i])
                token["market_cap"] = str(market_cap[i])
                token["market_rank"] = str(marketRank[i])
                database.update_token(token)
                database.neo4j_update_token(token)
                tokens_dict[addr[i]]["decimals"] = token.get("decimals")
                # print(token)

        json.dump(tokens_dict, f)

# update_token_credit_score()
