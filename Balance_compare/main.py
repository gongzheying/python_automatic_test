"""
Created on Jan 3, 2020

@author: wangyx
"""

from balance_compare.balance_compare import BalanceCompare

if __name__ == '__main__':

    clazz = BalanceCompare()
    clazz.run("LT", "2017-12-02", "nbs/nbs@10.188.21.147/isis2db")
