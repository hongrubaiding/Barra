# -*-coding:utf-8-*-
# Author:ZouHao
# email:1084848158@qq.com

import mylog as mylog_demo
from GetDataPack.GetDataMain import GetDataMain
from HandleFactor import HandleFactor
from datetime import datetime, timedelta
import pandas as pd


class BarraMain:
    def __init__(self):
        self.logger = mylog_demo.set_log('Barra')
        self.GetDataMainDemo = GetDataMain()
        self.HandleFactorDemo = HandleFactor(self.logger)
        self.calc_param = {"start_date": "2017-06-01", "end_date": "2019-12-31", "market_inner": 94455, "Period": "D"}
        self.year_trade = 252
        self.file_path = "D:\\AnacondaProject\\Barra\\因子数据\\"

    def get_calc(self, trade_date, next_date):
        df_total = self.GetDataMainDemo.get_index_weight(trade_date, index_code=self.calc_param['market_inner']
                                                         ).set_index("InnerCode").iloc[:300]
        total_code = df_total.index.tolist()
        df_next_return = self.GetDataMainDemo.get_stock_hq(start_date=next_date, end_date=next_date,
                                                           inner_code=total_code).iloc[0]
        style_factor_list = ["Growth", "EarningsYield", "Liquidity", "NonLinearSize", "Beta",
                             "RisidualVolatility", "BookToPrice", "Momentum", "Size"]
        calc_param = {}
        calc_param['trade_date'] = trade_date
        calc_param["df_total_init"] = df_total
        calc_param["year_trade"] = self.year_trade
        df_factol_list = []
        for factor in style_factor_list:
            self.logger.info("处理大类因子：%s;" % factor)
            se_factor = self.HandleFactorDemo.handle_factol_main(factor, calc_param=calc_param)
            df_factol_list.append(se_factor)
        df_factor = pd.concat(df_factol_list,axis=1,sort=True).dropna(axis=0)
        return df_factor


    def get_start(self):
        trade_date_list = self.GetDataMainDemo.get_trade_day(self.calc_param["start_date"], self.calc_param["end_date"],
                                                             self.calc_param["Period"])
        self.logger.info("起始日期：%s;截止日期:%s" % (trade_date_list[0], trade_date_list[-1]))
        for trade_date, next_date in zip(trade_date_list[:-1], trade_date_list[1:]):
            df_factor = self.get_calc(trade_date, next_date)
            df_factor.to_csv(self.file_path+"%s.csv"%trade_date)


if __name__ == "__main__":
    BarraMainDemo = BarraMain()
    BarraMainDemo.get_start()
