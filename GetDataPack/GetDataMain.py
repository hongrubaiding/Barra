# -- coding: utf-8 --
# Author:ZouHao
# email:1084848158@qq.com

from GetDataPack.ConnectDataBase import ConnectDataBase
import pandas as pd
from datetime import datetime


class GetDataMain:
    def __init__(self, ):
        ConnectDataBaseDemo = ConnectDataBase()
        self.conn = ConnectDataBaseDemo.connect_database(flag="JYDB-Formal")

    def format_str(self, code_list=[]):
        if len(code_list) > 1:
            result_str = str(tuple(code_list))
        else:
            result_str = "(%s)" % code_list[0]
        return result_str

    def get_trade_day(self, start_date='2013-06-01', end_date='2015-12-31', period='D', delta=-1):
        dic_period = {"W": "IfWeekEnd", "M": "IfMonthEnd", "Q": "IfQuarterEnd"}
        if delta == -1:
            if period not in dic_period:
                sql_str = '''SELECT TradingDate from QT_TradingDayNew WHERE IfTradingDay =1 AND SecuMarket=83 
                and TradingDate BETWEEN '%s' and '%s'; ''' % (start_date, end_date)
            else:
                sql_str = '''SELECT TradingDate from QT_TradingDayNew WHERE IfTradingDay =1 AND SecuMarket=83 and %s=1
                            and TradingDate BETWEEN '%s' and '%s'; ''' % (dic_period[period], start_date, end_date)

        else:
            sql_str = '''SELECT top %s TradingDate from QT_TradingDayNew WHERE IfTradingDay =1 AND SecuMarket=83 and 
            TradingDate<='%s' ORDER BY TradingDate desc;''' % (delta, end_date)
        trade_list = pd.read_sql(sql_str, self.conn)["TradingDate"].tolist()
        trade_list = [date_str.strftime("%Y-%m-%d") for date_str in trade_list]
        return trade_list

    def get_index_weight(self, trade_date='2021-09-22', index_code=94455):
        sql_str = '''SELECT A.InnerCode,A.Weight,B.CompanyCode,B.SecuAbbr,B.ListedDate FROM LC_IndexComponentsWeight A INNER JOIN
         SecuMain B on A.InnerCode=B.InnerCode  WHERE A.IndexCode=%s and A.EndDate in (SELECT max(DISTINCT EndDate)
        as EndDate FROM LC_IndexComponentsWeight WHERE EndDate<='%s' and IndexCode=%s);''' % (
            index_code, trade_date, index_code)
        df_stock = pd.read_sql(sql_str, self.conn)
        return df_stock

    def get_suspend_stock(self, date_str):
        sql_str = '''SELECT InnerCode from LC_SuspendResumption where SuspendDate<='%s' and ResumptionDate>'%s';''' % (
            date_str, date_str)
        stock_list = pd.read_sql(sql_str, self.conn)['InnerCode'].tolist()
        return stock_list

    def get_stock_hq(self, start_date='2013-06-01', end_date='2015-12-31', inner_code=[3, 6], flag='stock'):
        if flag == 'stock':
            sql_str = '''select InnerCode,TradingDay,ChangePCT from QT_StockPerformance where InnerCode in %s and
             TradingDay between '%s' and '%s';''' % (self.format_str(inner_code), start_date, end_date)
        else:
            sql_str = '''select InnerCode,TradingDay,ChangePCT from QT_IndexQuote where InnerCode in %s and
                         TradingDay between '%s' and '%s';''' % (self.format_str(inner_code), start_date, end_date)
        df_hq = pd.read_sql(sql_str, self.conn)
        if len(df_hq['InnerCode'].unique()) > 1:
            df_list = [temp_df.rename(columns={"ChangePCT": code}).set_index("TradingDay").drop("InnerCode", axis=1) for
                       code, temp_df in df_hq.groupby("InnerCode")]
            df = pd.concat(df_list, axis=1, sort=True)
        else:
            df = df_hq.rename(columns={"ChangePCT": df_hq['InnerCode'].iloc[0]}).set_index("TradingDay").drop(
                "InnerCode", axis=1)
        return df

    def get_QFinancialIndex(self, inner_code=[3, 6], trade_date='2012-09-22', fileds=[]):
        sql_str = '''select InnerCode,%s from LC_DIndicesForValuation where  InnerCode in %s and TradingDay='%s';''' % (
            ','.join(fileds), self.format_str(inner_code), trade_date)
        df = pd.read_sql(sql_str, self.conn)
        return df

    def get_LCQFinancialIndexNew(self, company_code=[3, 6], trade_date='2012-09-22'):
        sql_str = '''select CompanyCode,NetProfitGrowRate,OperatingRevenueYOY from LC_QFinancialIndexNew where EndDate 
        in (select  top 1 EndDate from LC_MainIndexNew where EndDate<'%s' order by  EndDate desc) and CompanyCode in 
        %s and Mark=2;''' % (trade_date, self.format_str(company_code))
        df = pd.read_sql(sql_str, self.conn)
        return df

    def get_turn_rate(self, inner_code=[3, 6], start_date='2021-09-01', end_date='2021-09-22', ):
        sql_str = '''select InnerCode,TradingDay,TurnoverRate from DZ_Performance where InnerCode in %s and
             TradingDay between '%s' and '%s';''' % (self.format_str(inner_code), start_date, end_date)
        df_hq = pd.read_sql(sql_str, self.conn)
        if len(df_hq['InnerCode'].unique()) > 1:
            df_list = [temp_df.rename(columns={"TurnoverRate": code}).set_index("TradingDay").drop("InnerCode", axis=1)
                       for
                       code, temp_df in df_hq.groupby("InnerCode")]
            df = pd.concat(df_list, axis=1, sort=True)
        else:
            df = df_hq.rename(columns={"TurnoverRate": df_hq['InnerCode'].iloc[0]}).set_index("TradingDay").drop(
                "InnerCode", axis=1)
        return df


if __name__ == "__main__":
    GetDataMainDemo = GetDataMain()
    GetDataMainDemo.get_trade_day()
