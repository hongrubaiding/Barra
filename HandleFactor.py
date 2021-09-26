# -- coding: utf-8 --
# Author:ZouHao
# email:1084848158@qq.com

from GetDataPack.GetDataMain import GetDataMain
import pandas as pd
import statsmodels.api as sm
from datetime import datetime, timedelta
import numpy as np


class HandleFactor(GetDataMain):
    def __init__(self, logger):
        GetDataMain.__init__(self)
        self.logger = logger
        self.global_dic = {}

    def handle_factol_main(self, factor='Beta', calc_param={}):
        if factor == 'Beta':
            se_factor = self.handle_beta(trade_date=calc_param["trade_date"], df_total_init=calc_param["df_total_init"],
                                         year_trade=calc_param["year_trade"])
        elif factor == "Size":
            se_factor = self.handle_size(calc_param["trade_date"], calc_param["df_total_init"])
        elif factor == "Momentum":
            se_factor = self.handle_momentum(calc_param["trade_date"], calc_param["df_total_init"],
                                             calc_param["year_trade"])
        elif factor == "BookToPrice":
            se_factor = self.handle_book_to_price(calc_param["trade_date"], calc_param["df_total_init"], )
        elif factor == "RisidualVolatility":
            se_factor = self.handle_risidual_volatility(calc_param["trade_date"], calc_param["df_total_init"], )
        elif factor == "NonLinearSize":
            se_factor = self.handle_non_linear_size(calc_param["trade_date"], calc_param["df_total_init"], )
        elif factor == "Liquidity":
            se_factor = self.handle_liquidity(calc_param["trade_date"], calc_param["df_total_init"],
                                              calc_param["year_trade"])
        elif factor == "EarningsYield":
            se_factor = self.handle_earnings_yield(calc_param["trade_date"], calc_param["df_total_init"], )
        elif factor == "Growth":
            se_factor = self.handle_growth(calc_param["trade_date"], calc_param["df_total_init"], )
        # elif factor == "Leverage":
        #     se_factor = self.handle_leverage(calc_param["trade_date"], calc_param["df_total_init"],)
        return se_factor

    def handle_leverage(self,trade_date, df_total_init):
        df_data = 0

    def handle_growth(self, trade_date, df_total_init):
        df_data = self.get_LCQFinancialIndexNew(company_code=df_total_init['CompanyCode'].tolist(),
                                          trade_date=trade_date).set_index("CompanyCode") / 100
        se_temp = ([0.5, 0.5] * df_data).sum(axis=1)
        se_temp.name = "Growth"
        temp_df = df_total_init[["CompanyCode"]].copy().reset_index().set_index("CompanyCode")
        df_temp = pd.concat([temp_df,se_temp],axis=1,sort=True)[["InnerCode","Growth"]].set_index("InnerCode")
        se_factor = df_temp["Growth"]
        return se_factor

    def handle_earnings_yield(self, trade_date, df_total_init):
        df_data = self.get_QFinancialIndex(inner_code=df_total_init.index.tolist(), trade_date=trade_date,
                                           fileds=['PCFTTM', "PE"]).set_index("InnerCode")
        se_factor = ([0.5, 0.5] * 1 / df_data).sum(axis=1)
        se_factor.name = "EarningsYield"
        return se_factor

    def handle_liquidity(self, trade_date, df_total_init, year_trade):
        # 去上市不满半年,次新股
        judge_date = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(183)).strftime("%Y-%m-%d")
        df_total = df_total_init[df_total_init['ListedDate'] <= judge_date]

        # 去停牌股
        suspend_code = self.get_suspend_stock(trade_date)
        not_suspend = [code for code in df_total.index if code not in suspend_code]
        df_total = df_total.loc[not_suspend]

        his_date_list = sorted(self.get_trade_day(end_date=trade_date, delta=year_trade))
        df_data = self.get_turn_rate(inner_code=df_total.index.tolist(), start_date=his_date_list[0],
                                     end_date=his_date_list[-1], )
        dic_factor = {}
        for code in df_data.columns:
            se_temp = df_data[code].fillna(0)
            se_value = 0.5 * np.log(se_temp.iloc[-21:].mean()) + 0.25 * np.log(
                se_temp.iloc[-63:].mean()) + 0.25 * np.log(se_temp.mean())
            dic_factor[code] = se_value
        se_factor = pd.Series(dic_factor)
        se_factor.name = "Liquidity"
        return se_factor

    def handle_non_linear_size(self, trade_date, df_total_init):
        TotalMVDf = self.global_dic.get("TotalMV", pd.DataFrame())
        if TotalMVDf.empty:
            TotalMVDf = self.get_QFinancialIndex(inner_code=df_total_init.index.tolist(), trade_date=trade_date,
                                                 fileds=['TotalMV']).set_index("InnerCode")
            self.global_dic["TotalMV"] = TotalMVDf

        se_value_ln3 = np.power(np.log(TotalMVDf), 3)["TotalMV"]
        se_value_ln3.name = "CubeLogSize"
        se_value_ln = np.log(TotalMVDf)["TotalMV"]
        se_value_ln.name = "LogSize"
        x = sm.add_constant(se_value_ln)  # 若模型中有截距，必须有这一步
        model = sm.OLS(se_value_ln3, x, ).fit()  # 构建最小二乘模型并拟合
        se_factor = model.resid
        se_factor.name = "NonLinearSize"
        return se_factor

    def handle_risidual_volatility(self, trade_date, df_total_init):
        HSIGMA = self.global_dic.get("HSIGMA", pd.Series())
        StockHQDf = self.global_dic.get("StockHQDf", pd.DataFrame())
        MarketIndexDf = self.global_dic.get("MarketIndexDf", pd.DataFrame())
        if HSIGMA.empty or StockHQDf.empty or MarketIndexDf.empty:
            self.logger.error("全局字典里，暂未计算HSIGMA/StockHQDf/MarketIndexDf！")
            return HSIGMA
        df_list = []
        for code in StockHQDf.columns:
            se_temp = StockHQDf[code].dropna()
            if len(se_temp) < 42:
                continue
            se_temp = se_temp.fillna(0)
            se_excess = se_temp - MarketIndexDf['Market']
            se_excess.name = code
            df_list.append(se_excess)
        excess_df = pd.concat(df_list, axis=1, sort=True).fillna(0)
        weight = (0.5 ** (np.arange(len(excess_df)) / 42))[::-1]
        weight = weight / weight.sum()
        DASTD = np.sqrt((weight.reshape(-1, 1) * np.power(excess_df, 2)).sum())

        use_code = []
        for code in StockHQDf.columns:
            se_temp = StockHQDf[code].dropna()
            if len(se_temp) < 10:
                continue
            use_code.append(code)
        use_hq_df = StockHQDf[use_code].fillna(0)

        df_list1 = []
        for monty_num in range(0, 12):
            if monty_num * 21 + 21 < use_hq_df.shape[0]:
                temp_df = use_hq_df.iloc[:monty_num * 21 + 21]
            else:
                temp_df = use_hq_df.copy()
            se_a = (np.log(1 + temp_df / 100).sub(np.log(1 + MarketIndexDf['Market'] / 100), axis=0)).sum()
            se_a.name = "累计月份%s" % (monty_num + 1)
            df_list1.append(se_a)
        df_cmra = pd.concat(df_list1, axis=1, sort=True).T
        CRMA = np.log(1 + df_cmra.max()) - np.log(1 + df_cmra.min())
        se_factor = CRMA * 0.15 + HSIGMA * 0.15 + DASTD * 0.7
        se_factor.name = "RisidualVolatility"
        return se_factor

    def handle_book_to_price(self, trade_date, df_total_init):
        df_data = self.get_QFinancialIndex(inner_code=df_total_init.index.tolist(), trade_date=trade_date,
                                           fileds=['PB']).set_index("InnerCode")
        se_factor = 1 / df_data["PB"]
        se_factor.name = "BookToPrice"
        return se_factor

    def handle_momentum(self, trade_date, df_total_init, year_trade):
        '''
        对于数据质量较好的个股，计算动量时采用了2年的数据;需要剔除未上市日期数据，但无需剔除停牌日期数据，并将权重归一化;
        若满足条件的数据样本小于42天，将其动量置为NaN
        :param trade_date:
        :param df_total_init:
        :return:
        '''
        his_date_list0 = sorted(self.get_trade_day(end_date=trade_date, delta=21))
        his_date_list = sorted(self.get_trade_day(end_date=his_date_list0[0], delta=year_trade * 2))

        # 去上市时间不足
        df_total = df_total_init[df_total_init['ListedDate'] <= his_date_list[0]]
        df_hq = self.get_stock_hq(start_date=his_date_list[0], end_date=his_date_list[-1],
                                  inner_code=df_total.index.tolist()).fillna(0)
        df_hq = np.log(1 + df_hq / 100)
        weight = (0.5 ** (np.arange(df_hq.shape[0]) / 126))[::-1]
        weight = weight / weight.sum()
        se_factor = (df_hq.T * weight).T.sum()
        se_factor.name = "Momentum"
        return se_factor

    def handle_size(self, trade_date, df_total_init):
        TotalMVDf = self.global_dic.get("TotalMV", pd.DataFrame())
        if TotalMVDf.empty:
            TotalMVDf = self.get_QFinancialIndex(inner_code=df_total_init.index.tolist(), trade_date=trade_date,
                                                 fileds=['TotalMV']).set_index("InnerCode")
            self.global_dic['TotalMV'] = TotalMVDf
        se_factor = np.log(TotalMVDf["TotalMV"])
        se_factor.name = 'Size'
        return se_factor

    def handle_beta(self, trade_date, df_total_init, year_trade):
        # 去上市不满半年,次新股
        judge_date = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(183)).strftime("%Y-%m-%d")
        df_total = df_total_init[df_total_init['ListedDate'] <= judge_date]

        # 去停牌股
        suspend_code = self.get_suspend_stock(trade_date)
        not_suspend = [code for code in df_total.index if code not in suspend_code]
        df_total = df_total.loc[not_suspend]

        his_date_list = sorted(self.get_trade_day(end_date=trade_date, delta=year_trade))
        df_hq = self.get_stock_hq(start_date=his_date_list[0], end_date=his_date_list[-1],
                                  inner_code=df_total.index.tolist())
        self.global_dic["StockHQDf"] = df_hq

        # 获取市场指数过去一年收益率
        MarketIndexDf = self.global_dic.get("MarketIndexDf", pd.DataFrame())
        if MarketIndexDf.empty:
            market_index = 1
            MarketIndexDf = self.get_stock_hq(start_date=his_date_list[0], end_date=his_date_list[-1],
                                              inner_code=[market_index], flag='index')
            MarketIndexDf.rename(columns={market_index: "Market"}, inplace=True)
            self.global_dic["MarketIndexDf"] = MarketIndexDf

        self.logger.info("计算beta用样本股票量：%s" % df_hq.shape[1])
        df_total_hq = pd.concat([df_hq, MarketIndexDf], axis=1, sort=True)
        se_init_m = df_total_hq.iloc[:, -1]
        dic_beta = {}
        dic_risid_std = {}
        for inner_code in df_total_hq.columns[:-1]:
            try:
                se_s = df_total_hq[inner_code].dropna()
                weight = (0.5 ** (np.arange(len(se_s)) / 63))[::-1]
                weight = weight / weight.sum()
                if len(df_total_hq[inner_code]) > len(se_s):
                    self.logger.info("当前股票：%s;可用样本天数：%s;总天数%s;" % (inner_code, len(se_s),
                                                                   len(df_total_hq[inner_code])))
                se_m = se_init_m.loc[se_s.index]
                x = sm.add_constant(se_m)  # 若模型中有截距，必须有这一步
                # model = sm.OLS(se_y, x,).fit()  # 构建最小二乘模型并拟合
                model = sm.WLS(se_s, x, weights=weight).fit()
                dic_beta[inner_code] = model.params['Market']
                dic_risid_std[inner_code] = model.resid.std()
            except:
                self.logger.error("当前股票:%s,异常！！！" % inner_code)
        se_factor = pd.Series(dic_beta, name='Beta')
        HSIGMA = pd.Series(dic_beta, name='RisidStd')
        self.global_dic["HSIGMA"] = HSIGMA
        return se_factor
