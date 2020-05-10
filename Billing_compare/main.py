"""
Created on Jan 3, 2020

@author: wangyx
"""

from billing_compare.billing_compare import BillingCompare

if __name__ == '__main__':
    
    clazz = BillingCompare()
    clazz.run("TP", "2017-12-03", "nbs_common/nbs_common@10.188.21.147/isis2db")
