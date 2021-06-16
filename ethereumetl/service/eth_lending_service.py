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

from web3 import Web3
from web3.exceptions import BadFunctionCallOutput

from artifacts.abi_pi.lending_pool_abi import LENDING_POOL_ABI
from artifacts.abi_pi.vToken_abi import VTOKEN_ABI
from config.constant import WalletConstant, LendingTypeConstant, LoggerConstant, LendingPoolConstant
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

logger = logging.getLogger(LoggerConstant.EthLendingService)


class EthLendingService(object):
    def __init__(self, web3, function_call_result_transformer=None, provider_uris=None):
        self._web3 = web3
        self.web3s = [web3]
        if provider_uris:
            for provider in provider_uris:
                batch_web3_provider = ThreadLocalProxy(lambda: get_provider_from_uri(provider, batch=True))
                w3 = Web3(batch_web3_provider)
                self.web3s.append(w3)
        self._function_call_result_transformer = function_call_result_transformer
        self.token_contract = {}
        # self.mapping_abi = {
        #     TokenABIConfig.ERC20: ERC20_ABI,
        #     TokenABIConfig.VTOKEN: VToken_ABI
        # }
        self.mapping_handler = {
            LendingTypeConstant.VTOKEN: self.get_lending_info_v_token,
            LendingTypeConstant.LENDING_POOL: self.get_lending_info_pool
        }

    def get_lending_info(self, contract_address, address, block_identifier="latest",
                         token_type=LendingTypeConstant.VTOKEN):
        """

        :rtype: balance, pre_balance, supply, borrow, unit_token
        """
        handler = self.mapping_handler[token_type]
        if not handler:
            logger.warning(f"getting lending info for smart contract type :{token_type} has not supported ")
            return
        return handler(contract_address, address, block_identifier)

    def get_lending_info_v_token(self, contract_address, address, block_identifier="latest"):
        """

        :rtype: balance, pre_balance, supply, borrow, unit_token
        """
        # start_time = time()

        if address == WalletConstant.address_nowhere:
            return
        # w3 = random.choice(self.web3s)
        checksum_address = self._web3.toChecksumAddress(address)
        checksum_token_address = self._web3.toChecksumAddress(contract_address)
        contract_address = str(checksum_token_address).lower()
        if not self.token_contract.get(contract_address):
            self.token_contract[contract_address] = self._web3.eth.contract(address=checksum_token_address,
                                                                            abi=VTOKEN_ABI)
        contract = self.token_contract.get(contract_address)

        try:
            balance = self._get_first_result(contract.functions.balanceOf(checksum_address),
                                             block_identifier=block_identifier)
            pre_balance = self._get_first_result(contract.functions.balanceOf(checksum_address),
                                                 block_identifier=block_identifier - 1)

            supply = self._get_first_result(contract.functions.balanceOfUnderlying(checksum_address),
                                            block_identifier=block_identifier)
            borrow = self._get_first_result(contract.functions.borrowBalanceCurrent(checksum_address),
                                            block_identifier=block_identifier)
            unit_token = contract_address
            return balance, pre_balance, supply, borrow, unit_token

        except Exception as e:
            logger.error(e)
            print(e)
            # data_balance["data"] = None
            return None, None, None, None, None

    def get_lending_info_pool(self, contract_address, address, block_identifier="latest"):
        """

        :rtype: balance, pre_balance, supply, borrow, unit_token
        """
        if address == WalletConstant.address_nowhere:
            return
            # w3 = random.choice(self.web3s)
        checksum_address = self._web3.toChecksumAddress(address)
        checksum_token_address = self._web3.toChecksumAddress(contract_address)
        contract_address = str(checksum_token_address).lower()
        if not self.token_contract.get(contract_address):
            self.token_contract[contract_address] = self._web3.eth.contract(address=checksum_token_address,
                                                                            abi=LENDING_POOL_ABI)
        contract = self.token_contract.get(contract_address)

        try:
            totalCollateralETH, totalDebtETH, availableBorrowsETH, currentLiquidationThreshold, ltv, healthFactor = self._get_first_result(
                contract.functions.getUserAccountData(checksum_address),
                block_identifier=block_identifier)
            supply = totalCollateralETH / 10 ** LendingPoolConstant.DECIMALS
            borrow = totalDebtETH / 10 ** LendingPoolConstant.DECIMALS
            balance = 0
            pre_balance = 0
            unit_token = "usd"
            return balance, pre_balance, supply, borrow, unit_token

        except Exception as e:
            logger.error(e)
            print(e)
            # data_balance["data"] = None
            return None, None

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
