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

from web3.exceptions import BadFunctionCallOutput

from artifacts.abi_pi.vToken_abi import VToken_ABI
from ethereumetl.erc20_abi import ERC20_ABI

logger = logging.getLogger('eth_token_service')


class EthTokenTypeService(object):
    ERC20 = "ERC20"
    VTOKEN = "VTOKEN"

    def __init__(self, web3, function_call_result_transformer=None):
        self._web3 = web3
        self._function_call_result_transformer = function_call_result_transformer
        self.abi_map = {
            "ERC20": ERC20_ABI,
            "VTOKEN": VToken_ABI
        }
        self.mapping_handler = {
            "ERC20": self._erc20_handler,
            "VTOKEN": self._vtoken_hanlder
        }

    def get_token(self, smart_contract_address, token_type="ERC20"):
        handler = self.mapping_handler[token_type]
        if handler:
            return handler(smart_contract_address, token_type)

    def _vtoken_hanlder(self, smart_contract_address, token_type):

        checksum_address = self._web3.toChecksumAddress(smart_contract_address)
        contract = self._web3.eth.contract(address=checksum_address, abi=self.abi_map[token_type])

        total_supply = self._get_first_result(contract.functions.totalSupply())
        total_borrow = self._get_first_result(contract.functions.totalBorrowsCurrent())
        block_num = self._web3.eth.blockNumber

        token = {
            "total_supply": total_supply,
            "total_borrow": total_borrow,
            "block_number": block_num
        }

        return token

    def _erc20_handler(self, smart_contract_address, token_type):

        return

    def get_account_info(self, account_address, smart_contract_address, token_type, block_identifier="latest"):
        checksum_address = self._web3.toChecksumAddress(smart_contract_address)
        checksum_account_address = self._web3.toChecksumAddress(account_address)
        contract = self._web3.eth.contract(address=checksum_address, abi=self.abi_map[token_type])
        balance = self._get_first_result(contract.functions.balanceOf(checksum_account_address),
                                         block_identifier=block_identifier)
        supply = self._get_first_result(contract.functions.balanceOfUnderlying(checksum_account_address),
                                        block_identifier=block_identifier)
        borrow = self._get_first_result(contract.functions.borrowBalanceCurrent(checksum_account_address),
                                        block_identifier=block_identifier)
        if block_identifier == "latest":
            block_identifier = self._web3.eth.blockNumber
        if not supply:
            supply = 0
        if not borrow:
            borrow = 0
        account_info = {
            "balance": str(balance),
            "supply": str(supply),
            "borrow": str(borrow),
            "block_number": block_identifier
        }
        return account_info

    def get_balance(self, token_address, address, block_identifier="latest"):
        if address == "0x0000000000000000000000000000000000000000":
            return
        checksum_token_address = self._web3.toChecksumAddress(token_address)
        checksum_address = self._web3.toChecksumAddress(address)
        contract = self._web3.eth.contract(address=checksum_token_address, abi=ERC20_ABI)

        try:
            balance = contract.functions.balanceOf(checksum_address).call(block_identifier=block_identifier)
            return balance
        except Exception as e:
            logger.error(e)
            print(e)
            return None

    def _get_first_result(self, *funcs, block_identifier="latest"):
        for func in funcs:

            result = self._call_contract_function(func, block_identifier)
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
            block_identifier=block_identifier)

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
            # logger.exception('An exception occurred in function {} of contract {}. '.format(func.fn_name, func.address)
            #                  + 'This exception can be safely ignored.')
            return default_value
        else:
            # logger.exception(ex)
            return default_value


ASCII_0 = 0


def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content
