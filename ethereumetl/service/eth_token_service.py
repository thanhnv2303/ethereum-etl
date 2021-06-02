# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
from time import time

from web3.exceptions import BadFunctionCallOutput

from ethereumetl.domain.token import EthToken
from ethereumetl.erc20_abi import ERC20_ABI

logger = logging.getLogger('eth_token_service')


class EthTokenService(object):
    def __init__(self, web3, function_call_result_transformer=None):
        self._web3 = web3
        self._function_call_result_transformer = function_call_result_transformer

    def get_token(self, token_address):
        checksum_address = self._web3.toChecksumAddress(token_address)
        contract = self._web3.eth.contract(address=checksum_address, abi=ERC20_ABI)

        symbol = self._get_first_result(contract.functions.symbol(), contract.functions.SYMBOL())
        name = self._get_first_result(contract.functions.name(), contract.functions.NAME())
        decimals = self._get_first_result(contract.functions.decimals(), contract.functions.DECIMALS())
        total_supply = self._get_first_result(contract.functions.totalSupply())

        token = EthToken()
        token.address = token_address
        token.symbol = symbol
        token.name = name
        token.decimals = decimals
        token.total_supply = total_supply

        return token

    async def get_balance(self, token_address, address, block_identifier="latest", data_balance=None):
        # start_time = time()

        if address == "0x0000000000000000000000000000000000000000":
            return
        checksum_token_address = self._web3.toChecksumAddress(token_address)
        checksum_address = self._web3.toChecksumAddress(address)
        contract = self._web3.eth.contract(address=checksum_token_address, abi=ERC20_ABI)

        try:

            balance = self._get_first_result(contract.functions.balanceOf(checksum_address),
                                             block_identifier=block_identifier)
            # print("balance ",balance)
            data_balance["data"] = balance
            # end_time = time()
            # print("time to call get balance of " + address + " at contract " + token_address + " is " + str(
            #     end_time - start_time))
            return balance

        except Exception as e:
            logger.error(e)
            print(e)
            data_balance["data"] = 0
            return None

    def _get_first_result(self, *funcs, block_identifier="latest"):
        for func in funcs:

            result = self._call_contract_function(func, block_identifier=block_identifier)
            if result is not None:
                return result
        return None

    def _call_contract_function(self, func, block_identifier="latest"):
        # BadFunctionCallOutput exception happens if the token doesn't implement a particular function
        # or was self-destructed
        # OverflowError exception happens if the return type of the function doesn't match the expected type
        result = call_contract_function(
            func=func,
            ignore_errors=(BadFunctionCallOutput, OverflowError, ValueError),
            default_value=None,
            block_identifier=block_identifier
        )

        if self._function_call_result_transformer is not None:
            return self._function_call_result_transformer(result)
        else:
            return result


def call_contract_function(func, ignore_errors, default_value=None, block_identifier="latest"):
    try:

        result = func.call(block_identifier=block_identifier)
        return result
    except Exception as ex:
        if type(ex) in ignore_errors:
            logger.exception('An exception occurred in function {} of contract {}. '.format(func.fn_name, func.address)
                             + 'This exception can be safely ignored.')
            return default_value
        else:
            logger.exception(ex)
            return default_value
