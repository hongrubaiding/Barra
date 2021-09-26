# -- coding: utf-8 --
# Author:ZouHao
# email:1084848158@qq.com

from FundReportCalc.GetDataPack.ConnectDataBase import ConnectDataBase
import pandas as pd
import numpy as np
from datetime import datetime
from FundReportCalc.GetDataPack.GetFundDataHGS import GetFundDataHGS

import warnings

warnings.filterwarnings("ignore")


class GetFundData:
    def __init__(self, logger, flag="JYDB-Formal"):
        connect_database_demo = ConnectDataBase()
        self.conn = connect_database_demo.connect_database(flag=flag)
        self.logger = logger
        self.GetFundDataHGSDemo = GetFundDataHGS(self)
        self.grdb_conn =  connect_database_demo.connect_database(flag=
                                                                 'GRDB-Formal')

    def get_fund_base_info(self, inner_code, report_date):
        """
        获取季报披露仓位信息
        :param code:
        :return:
        """
        sql_str = '''SELECT InnerCode,InfoPublDate,ReportDate,RINOfStock,
        RINOfNational FROM MF_AssetAllocationNew WHERE InnerCode=%s and 
        ReportDate='%s';'''% (inner_code, report_date)
        df = pd.read_sql(sql_str, self.conn)

        sql1_str = '''select A.SecAssetCatCode,A.SecAssetCatName,
        A.ThirdAssetCatCode,A.CancelDate,B.Type,B.EstablishmentDate,
        B.InvestmentType from MF_JYFundType A inner join MF_FundArchives B on 
        A.InnerCode=B.InnerCode where A.Standard=75 and A.InnerCode=%s and 
        (A.CancelDate>='%s' or A.CancelDate is null);''' % (inner_code, 
        report_date)
        df1 = pd.read_sql(sql1_str, self.conn)
        return df, df1

    def get_trade_date(self, start_date, end_date):
        """
        后去指数日期，用于计算交易日
        :param start_date:
        :param end_date:
        :return:
        """
        sql_str = '''select TradingDate from QT_TradingDayNew where 
        SecuMarket=83 and TradingDate between '%s' and '%s' and IfTradingDay=1
        ''' % (start_date, end_date)
        df = pd.read_sql(sql_str, self.conn)
        return df

    def adjust_ratioinnv(self, df):
        if df[df['RatioInNV'] == 0].empty:
            return df

        max_index = df['RatioInNV'].idxmax()
        temp_list = []
        for index in df[df['RatioInNV'] == 0].index:
            temp_df1 = df.loc[index, :].copy()
            temp_df1['RatioInNV'] = df.loc[index]['MarketValue'] * df.loc[
                max_index]['RatioInNV'] / df.loc[max_index]['MarketValue']
            temp_df1 = pd.DataFrame(temp_df1).T
            temp_list.append(temp_df1)
        new_df = pd.concat([df[df['RatioInNV'] != 0], pd.concat(temp_list, 
             axis=0, sort=True)],axis=0, sort=True)
        return new_df

    def get_fund_pub_indus_info(self, inner_code, report_date,hsg_flag=False):
        """
        获取季报披露的行业投资比例
        :param inner_code:
        :return:
        """
        if hsg_flag:
            df = self.GetFundDataHGSDemo.get_fund_pub_indus_info(inner_code, 
                                                                 report_date)
        else:
            sql_str = '''select InnerCode,InfoPublDate,ReportDate,IndustryName,
            InvestType,RatioInNV,InduStandard,IndustryCode,InduDiscCode,
            MarketValue  from MF_InvestIndustry where InnerCode=%s and 
            InduDiscCode is not null and InduStandard=22 and ReportDate='%s'
            ;''' % (inner_code, report_date)
            df = pd.read_sql(sql_str, self.conn)
            if df.empty:
                return df
            df = self.adjust_ratioinnv(df)

        if 3 in df['InvestType'].tolist():
            if len(df['InduDiscCode'].unique()) != df.shape[0]:
                df_list = []
                for indudis_code, temp_df1 in df.groupby("InduDiscCode"):
                    if temp_df1.shape[0] == 1:
                        df_list.append(temp_df1)
                        continue
                    new_temp = temp_df1[temp_df1["InduDiscCode"] == 
                                        indudis_code].copy()
                    new_temp["RatioInNV"] = temp_df1[temp_df1["InduDiscCode"] 
                                          == indudis_code]["RatioInNV"].sum()
                    new_temp = new_temp[new_temp["InvestType"] == 3]
                    df_list.append(new_temp)
                df = pd.concat(df_list, axis=0, sort=True)
        return df

    def get_fund_net_value(self, inner_code):
        """
        获取复权单位净值
        :param inner_code:
        :return:
        """
        sql_str = '''select InnerCode,TradingDay,UnitNVRestored from 
        MF_FundNetValueRe where InnerCode=%s order by TradingDay;
        ''' % inner_code
        df = pd.read_sql(sql_str, self.conn)
        return df
    
    def adjust_industry_change(self,df,df1):
        InfoPublDate = df['ReportDate'].unique()[0]
        temp_df1 = df.copy().set_index('StockInnerCode', drop=True)
        have_list = list(set(temp_df1.index).intersection(df1.index))
        temp_df2 = df1.loc[have_list, :]
        
        temp_df2 = temp_df2.fillna(np.nan)
        df_not_change_industry = temp_df2[pd.isna(temp_df2['CancelDate'])]
        df_change_industry = temp_df2[~ pd.isna(temp_df2['CancelDate'])
                                      ].sort_values("CancelDate",ascending=True)

        much_classify_commany = {}
        for CompanyCode, temp_df_industry in df_change_industry.groupby(
                "CompanyCode"):
            # 取距离季报发布最近，取消日期大于季报发布日期的分类
            total_cancel_date = temp_df_industry['CancelDate'].tolist()
            cancel_date = ''

            for cancel_date_num in range(0, len(total_cancel_date)):
                if (total_cancel_date[cancel_date_num] - InfoPublDate).days>=0:
                    cancel_date = total_cancel_date[cancel_date_num]
                    break

            if cancel_date:
                use_df = temp_df_industry[temp_df_industry['CancelDate']==
                                          cancel_date]
                use_df['CompanyCode'] = CompanyCode
                much_classify_commany[CompanyCode] = use_df
                
        much_classify_df = pd.DataFrame()
        if much_classify_commany.values():
            much_classify_df = pd.concat(much_classify_commany.values(), 
                                         axis=0, sort=True)
        not_chage_list = list(set(df_not_change_industry['CompanyCode']
                                  ).difference(set(much_classify_commany)))
        abselute_not_change_industry = df_not_change_industry.set_index(
            'CompanyCode', drop=False).loc[not_chage_list, :]
        
        if not much_classify_df.empty:
            df_not_change_industry_new = pd.concat([abselute_not_change_industry,
                              much_classify_df], axis=0,sort=True)
        else:
            df_not_change_industry_new = abselute_not_change_industry

        df_not_change_industry_new = df_not_change_industry_new.set_index(
            'StockInnerCode', drop=False)
        row_loc = list(set(df_not_change_industry_new['StockInnerCode'
                                     ].index).intersection(temp_df1.index))
   
        stock_indus_df = pd.concat([df_not_change_industry_new.loc[row_loc],
        temp_df1.loc[row_loc].drop("CompanyCode", axis=1)],axis=1,sort=True)
        return stock_indus_df

    def handle_industry_change(self, df, df1):
        df_list = []
        InfoPublDate = df['InfoPublDate'].unique()[0]
        if len(df['ReportDate'].unique()) > 1:
            self.logger.error('''current InfoPublDate:%s, exist much ReportDat:
         %s''' % (pd.to_datetime(InfoPublDate).strftime("%Y-%m-%d"), list(df[
                    'ReportDate'].unique())))
            return pd.DataFrame()

        temp_df1 = df.copy().set_index('StockInnerCode', drop=True)
        have_list = list(set(temp_df1.index).intersection(df1.index))
        temp_df2 = df1.loc[have_list, :]


        if len(temp_df1.index.unique()) == len(temp_df2.index.unique()
                         ) and temp_df1.shape[0] == temp_df2.shape[0]:
            # 不存在股票行业分类有过变动
            stock_indus_df = pd.concat([temp_df1, temp_df2], axis=1, sort=True)
        else:
            stock_indus_df = self.adjust_industry_change(df,df1)
       
        stock_indus_df['InfoPublDate'] = InfoPublDate
        stock_indus_df.drop('CompanyCode', axis=1, inplace=True)
        df_list.append(stock_indus_df.reset_index(drop=True))
        if len(df_list)>1:
            df = pd.concat(df_list, axis=0, sort=True, ignore_index=True)
        else:
            df = df_list[0]
        return df

    def combine_industy_classify(self, df, report_date,hsg_flag=False):
        """
        根据披露的重仓股数据，获取对应的行业分类结果
        :param df:
        :return:
        """
        if hsg_flag:
            df_result = self.GetFundDataHGSDemo.combine_industy_classify(df, 
                                                          report_date)
        else:
            sql_str1 = '''select InnerCode,CompanyCode from SecuMain where 
            InnerCode in %s;''' % self.format_str(df['StockInnerCode'].tolist())
            
            df1 = pd.read_sql(sql_str1, self.conn).set_index('CompanyCode', 
                                                             drop=True)
   
            sql_str2 = '''select  CompanyCode,FirstIndustryName,CancelDate,
            FirstIndustryCode,Standard from DZ_ExgIndustry where CompanyCode in
            %s and Standard=22 and (InfoSource is null or InfoSource!=
            '临时公告') and (CancelDate is null or CancelDate>='%s')'''%(
            self.format_str(df1.index.tolist()),report_date)
            df2 = pd.read_sql(sql_str2, self.conn).set_index('CompanyCode', 
                                                             drop=False)
    
            lack_classify_list = df1.loc[list(set(df1.index).difference(set(
                df2.index))), :].index.tolist()
            if lack_classify_list:
                self.logger.error("heavy poc lack SEC classify CompanyCode,!!!")
                self.logger.error("%s CompanyCode" % lack_classify_list)
                return pd.DataFrame()
            use_code = [code for code in df1.index.tolist() if code not in
                        lack_classify_list]
            use_df = pd.concat([df2, df1.loc[use_code, :]], sort=True, axis=1)
    
            use_df.rename(columns={"InnerCode": "StockInnerCode"}, inplace=True)
            use_df = use_df.set_index('StockInnerCode', drop=False)
            df_result = self.handle_industry_change(df, use_df)
        return df_result

    def get_fund_poc_info(self, base_info, inner_code, report_date,hsg_flag=False):
        """
        获取季报披露的重仓股数据
        :param inner_code:
        :return:
        """
        if hsg_flag:
            df = self.GetFundDataHGSDemo.get_fund_poc_info(base_info, 
                                                inner_code, report_date)
        else:
            sql_str1 = '''select A.InfoPublDate,A.ReportDate,A.StockInnerCode,
            A.RatioInNV,A.MarketValue,B.CompanyCode,B.SecuCode,B.SecuAbbr,
            B.ListedSector from MF_KeyStockPortfolio A inner join SecuMain B on
            A.StockInnerCode=B.InnerCode where A.InnerCode=%s and A.ReportDate
            ='%s' order by InfoPublDate desc;'''% (inner_code, report_date)
    
            sql_str2 = '''select A.InfoPublDate,A.ReportDate,A.StockInnerCode,
            A.RatioInNV,A.MarketValue,B.CompanyCode,B.SecuCode,B.SecuAbbr,
            B.ListedSector from MF_KeyStockPortfolio A inner join SecuMain B on
            A.StockInnerCode=B.InnerCode where A.InnerCode=%s and A.ReportDate=
            '%s' and A.InvestType=3 order by InfoPublDate desc;''' % (
                           inner_code, report_date)
    
            if base_info['SecAssetCatCode'] != 1102 or base_info['Type'] == 8:
                df = pd.read_sql(sql_str1, self.conn)
            else:
                df = pd.read_sql(sql_str2, self.conn)
                if df.empty:
                    df = pd.read_sql(sql_str1, self.conn)
                    
        df_result = pd.DataFrame()
        
        if df.shape[0]>1:
            df_result = self.combine_industy_classify(df,report_date,
                                                      hsg_flag=hsg_flag)
        return df_result

    def get_fund_posresult_industy(self,inner_code_list,report_date,standard=37):
        sql_str = '''select B.InnerCode,A.CompanyCode,A.FirstIndustryCode,
        A.FirstIndustryName,A.CancelDate from DZ_ExgIndustry A inner join 
        SecuMain B on A.CompanyCode=B.CompanyCode and B.InnerCode in %s and 
        A.Standard=%s and (A.CancelDate>'%s' or A.CancelDate is null);''' % (
                      str(tuple(inner_code_list)), standard, report_date)

        df = pd.read_sql(sql_str, self.conn).set_index("InnerCode")
        df["StockInnerCode"] = df.index.tolist()
        classify_se = df['StockInnerCode'].value_counts()
        single_df = df.loc[classify_se[classify_se == 1].index]
        much_classify_df = df.loc[classify_se[classify_se > 1].index]
        much_df = much_classify_df[~np.isnan(much_classify_df['CancelDate'])]
       
        df_list = [temp_df[temp_df["CancelDate"] == temp_df["CancelDate"].min()
          ] for StockInnerCode, temp_df in much_df.groupby("StockInnerCode")]
        result_df = pd.concat([single_df, pd.concat(df_list, axis=0, sort=True)
                               ], axis=0, sort=True)
        return result_df

    def get_fund_total_poc_info(self, inner_code,report_date, standard):
        """
        获取年报半年报详细持仓数据
        :param inner_code:
        :return:
        """

        sql_str = '''select A.InnerCode,A.InfoPublDate,A.ReportDate,
        A.StockInnerCode,A.RatioInNV,B.SecuAbbr,B.SecuCode,B.CompanyCode from 
        MF_StockPortfolioDetail A inner join SecuMain B on A.StockInnerCode=
        B.InnerCode where A.InnerCode=%s and A.ReportDate='%s' ''' % (
        inner_code,report_date)
        df_detail_info = pd.read_sql(sql_str, self.conn)

        total_companycode = df_detail_info['CompanyCode'].tolist()
        sql_str = '''select CompanyCode,InfoPublDate,Standard,CancelDate,
        FirstIndustryCode,FirstIndustryName from DZ_ExgIndustry where Standard
        =%s and CompanyCode in %s and (CancelDate >'%s' or CancelDate is null)
        ;''' % (standard, str(tuple(total_companycode)),report_date)
        df_detail_industry_info = pd.read_sql(sql_str, self.conn).set_index(
            "CompanyCode", )

        industry_classify_num_se = df_detail_industry_info.index.value_counts()
        have_not_change_code = industry_classify_num_se[industry_classify_num_se==1
                                                        ].index.tolist()
        have_change_code = industry_classify_num_se[industry_classify_num_se!=1
                                                    ].index.tolist()
        havenot_industry_change_df = df_detail_industry_info.loc[
        have_not_change_code][["FirstIndustryCode", "FirstIndustryName",
                               "Standard"]]
        have_industry_change_df = df_detail_industry_info.loc[have_change_code][
            ["FirstIndustryCode", "FirstIndustryName","CancelDate","Standard"]]

        temp_industry_df = df_detail_info.copy().set_index("CompanyCode")
        temp_not_change_code = list(set(have_not_change_code).intersection(
            temp_industry_df.index.tolist()))
        temp_not_change_df = pd.DataFrame()
        if temp_not_change_code:
            temp_not_change_df = pd.concat([temp_industry_df.loc[
                temp_not_change_code], havenot_industry_change_df.loc[
         temp_not_change_code]],axis=1, sort=True)
            
        temp_change_code = list(set(have_change_code).intersection(
            temp_industry_df.index.tolist()))
        temp_change_df = pd.DataFrame()
        if temp_change_code:
            change_df = have_industry_change_df.loc[temp_change_code].copy()
            change_df["StockCompanyCode"] = change_df.index.tolist()
            df_list = []
            for company_code, temp_classify_df in change_df.groupby(
                    "StockCompanyCode"):
                temp_df1 = temp_classify_df[~np.isnan(temp_classify_df[
                    'CancelDate'])]
                target_date = self.get_target_date(date_list=temp_df1[
                    "CancelDate"].tolist(),current_date=report_date)

                use_df = temp_classify_df[temp_classify_df["CancelDate"
                     ]==target_date].drop("StockCompanyCode",axis=1)
                df_list.append(use_df)
            total_use_df = pd.concat(df_list, axis=0, sort=True)
            temp_change_df = pd.concat([temp_industry_df.loc[temp_change_code],
                                        total_use_df], axis=1, sort=True)
        temp_total_industry_df = pd.concat([temp_not_change_df, temp_change_df
                                            ], axis=0, sort=True)
        temp_total_industry_df["CompanyCode"] = temp_total_industry_df.index.tolist()
        return temp_total_industry_df

    def get_target_date(self, date_list, current_date):
        """
        获取日期列表date_list中，距离当前日期最近，但小于当前日期的日期
        :param date_list:
        :param current_date:
        :return:
        """
        target_loc = np.nan
        if len(date_list) == 1:
            target_loc = date_list[0]
            return target_loc
        date_list_temp = date_list.copy()
        date_list_temp.sort()
        date_se = datetime.strptime(current_date,"%Y-%m-%d")-pd.Series(date_list)
        date_int_se = pd.Series([cha_day.days for cha_day in date_se])
        if not date_int_se[date_int_se < 0].empty:
            target_loc_index = date_int_se[date_int_se < 0].idxmax()

            target_loc = date_list[target_loc_index]
        return target_loc

    def get_stock_bool_info_new(self, before_trade_date, info_pub_date,
                                heav_code=[],hsg_flag=False):
        
        df_hgs_stock = pd.DataFrame()
        if hsg_flag:
            df_hgs_stock = self.GetFundDataHGSDemo.get_stock_bool_info_new(
                before_trade_date, info_pub_date,heav_code)
        
        sql_str = '''select A.InnerCode, A.CompanyCode, A.SecuCode, A.SecuAbbr,
        A.SecuCategory,B.Standard,B.FirstIndustryCode,B.FirstIndustryName,
        B.CancelDate from SecuMain A inner join DZ_ExgIndustry B on 
        A.CompanyCode=B.CompanyCode where A.SecuMarket in (83,90) and 
        A.SecuCategory in (1,41)  and A.ListedDate<='%s' and B.Standard in (22,37);
        ''' % before_trade_date.strftime("%Y-%m-%d")
        df_total_stock = pd.read_sql(sql_str, self.conn)

        df_total_stock['StockInnerCode'] = df_total_stock["InnerCode"]
        df_total_stock.set_index("StockInnerCode", inplace=True)
        unusual_code = [code for code in heav_code if code not in 
           df_hgs_stock.index.tolist() and code not in 
           df_total_stock.index.tolist()]
        if unusual_code:
            self.logger.error("heav pos stock have wrong info,%s" % unusual_code)
            return pd.DataFrame(), '',pd.DataFrame()

        classify_se = df_total_stock["InnerCode"].value_counts()
        # 过滤只有证监会、中信行业一个分类的股票
        df_total_stock1 = df_total_stock.loc[classify_se[classify_se > 1].index]
        # 过滤取消日期小于当前发布日期的分类
        df_total_stock2 = df_total_stock1[
            (df_total_stock1['CancelDate'] > pd.to_datetime(info_pub_date
             ).strftime("%Y-%m-%d")) | np.isnan(df_total_stock1['CancelDate'])]
        df_classify_list = []
        for Standard, temp_df in df_total_stock2.groupby("Standard"):
            temp_classify = temp_df['InnerCode'].value_counts()
            single_df = temp_df.loc[temp_classify[temp_classify == 1].index]

            much_classify_df = temp_df.loc[temp_classify[temp_classify > 1].index]
            if not much_classify_df.empty:
                much_df_list = [stock_df[stock_df['CancelDate'] == stock_df[
               'CancelDate'].min()] for innner_code, stock_df in 
                    much_classify_df.groupby("InnerCode")]
                temp_classify_df = pd.concat([single_df, pd.concat(much_df_list,
                           sort=True, axis=0)], axis=0, sort=True)
            else:
                temp_classify_df = single_df
            df_classify_list.append(temp_classify_df)
        df_result = pd.concat(df_classify_list, axis=0, sort=True)

        industry_df = df_result[df_result["Standard"] == 37].copy()
        industry_df = industry_df.rename(
            columns={"FirstIndustryCode": "ZX_FirstIndustryCode",
                     "FirstIndustryName": "ZX_FirstIndustryName", "Standard":
                         "ZX_STandard3"})
        industry_df.set_index("InnerCode", inplace=True, drop=True)

        industry_df1 = df_result[df_result["Standard"] != 37][
            ["InnerCode", "FirstIndustryCode", "FirstIndustryName", "Standard"]
            ].copy()

        industry_df1 = industry_df1.rename(
            columns={"FirstIndustryCode": "SEC_FirstIndustryCode",
                     "FirstIndustryName": "SEC_FirstIndustryName", "Standard": 
                         "SEC_STandard22"})
        industry_df1.set_index("InnerCode", inplace=True, drop=True)
        df_final = pd.concat([industry_df, industry_df1], axis=1, sort=True, )

        sql_str1 = '''select InnerCode,TotalMV from DZ_Performance where 
        TradingDay='%s' and InnerCode in %s''' % ( before_trade_date, 
        tuple(df_final.index.tolist()))
        df_stock_quote = pd.read_sql(sql_str1, self.conn)
        df_stock_quote.set_index("InnerCode", inplace=True)
        heav_code_A = [code for code in heav_code if code not in 
                       df_hgs_stock.index.tolist()]
        df_stock_quote, lack_code = self.get_suspend_mv(df_stock_quote, 
                               heav_code_A, before_trade_date)

        total_stock_innercode = list(set(df_stock_quote.index.tolist()
                          ).intersection(set(df_final.index.tolist())))
        df_total_stock_info = pd.concat(
            [df_stock_quote.loc[total_stock_innercode], df_final.loc[
                total_stock_innercode]], axis=1, sort=True)
        return df_total_stock_info, lack_code,df_hgs_stock

    def get_suspend_mv(self, df_stock_quote, heav_code, before_trade_date):
        "停牌的重仓股数据处理"
        lack_code = [code for code in heav_code if code not in df_stock_quote.index]
        if lack_code:
            self.logger.info('''heav poc have suspend or othern condition 
                        stock: %s,date:%s'''% (lack_code, before_trade_date))
            df_list = []
            for code in lack_code:
                sql_str = '''select top 1 InnerCode,TotalMV from DZ_Performance
                where TradingDay<'%s' and InnerCode = %s order by TradingDay 
                desc''' % (before_trade_date, code)
                temp_stock_quote = pd.read_sql(sql_str, self.conn)
                df_list.append(temp_stock_quote)
            temp_df = pd.concat(df_list, axis=0, sort=True).set_index("InnerCode")
            df_stock_quote = pd.concat([df_stock_quote, temp_df],axis=0,sort=True)
        return df_stock_quote, lack_code

    def get_stock_timechange_pct(self,before_trade_date, after_trade_date, 
                                 lack_df, inner_code_list=[]):
        df_stock_quote = self.get_stock_quote(inner_code_list, before_trade_date, 
          after_trade_date).set_index("InnerCode", drop=False)
        

        if not lack_df.empty:
            not_suspend_stock = list(set(df_stock_quote.index).difference(set(
                lack_df.index)))
            lack_indsutry_code = lack_df['ZX_FirstIndustryCode'].unique()
            self.logger.debug('''heav poc stock exist suspend:%s,indusrty 
                              return replace!!''' % lack_df.index.tolist())
            if len(lack_indsutry_code) > 1:
                sql_index_str = '''select A.IndexCode,B.IndustryCode from 
                LC_CorrIndexIndustry A inner join CT_IndustryType B on 
                A.IndustryCode=B.IndustryNum where B.IndustryCode in %s and 
                A.IndustryStandard=37 and A.IndexState!=3;''' % str(tuple(
                lack_indsutry_code))
                df_index = pd.read_sql(sql_index_str, self.conn)
                sql_index_str1 = '''select InnerCode,ChangePCT,TradingDay from 
                QT_IndexQuote where InnerCode in %s and  TradingDay between 
                '%s' and '%s';''' %(str(tuple(df_index['IndexCode'].unique())),
                                   before_trade_date,after_trade_date)
            else:
                sql_index_str = '''select A.IndexCode,B.IndustryCode from 
                LC_CorrIndexIndustry A inner join CT_IndustryType B on 
                A.IndustryCode=B.IndustryNum where B.IndustryCode = '%s' and 
                A.IndustryStandard=37 and A.IndexState!=3;
                ''' % lack_indsutry_code[0]
                df_index = pd.read_sql(sql_index_str, self.conn)
                sql_index_str1 = '''select InnerCode,ChangePCT,TradingDay from 
                QT_IndexQuote where InnerCode = %s and  TradingDay between '%s'
                and '%s';''' % (df_index['IndexCode'].unique()[0],
                before_trade_date,after_trade_date)

            df_index_quote = pd.read_sql(sql_index_str1, self.conn)
            df_lack_quote = []
            for lack_code in lack_df.index:
                ZX_IndustyrCode = lack_df.loc[lack_code]['ZX_FirstIndustryCode']
                index_code = df_index[df_index['IndustryCode']==ZX_IndustyrCode
                                      ]['IndexCode'].iloc[0]
                temp_index_quote = df_index_quote[df_index_quote['InnerCode'
                                                                 ]==index_code]
                temp_index_quote.loc[:, 'InnerCode'] = lack_code
                df_lack_quote.append(temp_index_quote)
            lack_quote_df = pd.concat(df_lack_quote, axis=0, sort=True
                                      ).set_index("InnerCode", drop=False)
            df_stock_quote = pd.concat([df_stock_quote.loc[not_suspend_stock, :
                          ], lack_quote_df], axis=0, sort=True)
            df_stock_quote.drop("InnerCode",axis=1,inplace=True)
        return df_stock_quote
    
    def get_stock_change_pct(self, before_trade_date, after_trade_date, 
                                 lack_df, inner_code_list=[]):
        """
        获取当前股票池在指定日期的涨跌幅，注意：inner_code_list中若有公司在当前
        日期未上市，取值为空
        :param before_trade_date:
        :param after_trade_date:
        :param inner_code_list:
        :return:
        """
        
        df_stock_quote = self.get_stock_quote(inner_code_list, 
         before_trade_date, after_trade_date).set_index("InnerCode",drop=False)
        
        if not lack_df.empty:
            # lack_df 为重仓A股中，停牌的股票
            not_suspend_stock = list(set(df_stock_quote.index).difference(set(
                lack_df.index)))
            lack_indsutry_code = lack_df['ZX_FirstIndustryCode'].unique()
            self.logger.info('''heav poc stock exist suspend:%s,indusrty return
                             replace!!''' % lack_df.index.tolist())
            if len(lack_indsutry_code) > 1:
                sql_index_str = '''select A.IndexCode,B.IndustryCode from 
                LC_CorrIndexIndustry A inner join CT_IndustryType B on 
                A.IndustryCode=B.IndustryNum where B.IndustryCode in %s and 
                A.IndustryStandard=37 and A.IndexState!=3;''' % str(tuple(
                lack_indsutry_code))
                df_index = pd.read_sql(sql_index_str, self.conn)
                sql_index_str1 = '''select InnerCode,ChangePCT,TradingDay from 
                QT_IndexQuote where InnerCode in %s and TradingDay between '%s'
                and '%s';''' % (str(tuple(df_index['IndexCode'].unique())),
                before_trade_date, after_trade_date)
            else:
                sql_index_str = '''select A.IndexCode,B.IndustryCode from 
                LC_CorrIndexIndustry A inner join CT_IndustryType B on 
                A.IndustryCode=B.IndustryNum where B.IndustryCode = '%s' and 
                A.IndustryStandard=37 and A.IndexState!=3;''' % lack_indsutry_code[0]
                df_index = pd.read_sql(sql_index_str, self.conn)
                sql_index_str1 = '''select InnerCode,ChangePCT,TradingDay from
                QT_IndexQuote where InnerCode = %s and  TradingDay between '%s'
                and '%s';'''% (df_index['IndexCode'].unique()[0],
                before_trade_date,after_trade_date)

            df_index_quote = pd.read_sql(sql_index_str1, self.conn)
            df_lack_quote = []
            for lack_code in lack_df.index:
                ZX_IndustyrCode = lack_df.loc[lack_code]['ZX_FirstIndustryCode']
                index_code = df_index[df_index['IndustryCode']==ZX_IndustyrCode
                                      ]['IndexCode'].iloc[0]
                temp_index_quote = df_index_quote[df_index_quote['InnerCode'
                                                                 ]==index_code]
                temp_index_quote.loc[:, 'InnerCode'] = lack_code
                df_lack_quote.append(temp_index_quote)
            lack_quote_df = pd.concat(df_lack_quote, axis=0, sort=True
                                      ).set_index("InnerCode", drop=False)
            df_stock_quote = pd.concat([df_stock_quote.loc[not_suspend_stock, :
                       ], lack_quote_df], axis=0, sort=True)
        return df_stock_quote

    def get_fund_timechange_pct(self, before_trade_date, after_trade_date, 
                                inner_code):
        sql_str1 = '''select TradingDay,NVRDailyGrowthRate from 
        MF_FundNetValueRe where TradingDay between '%s' and '%s' and InnerCode=
        %s;''' % (before_trade_date,after_trade_date, inner_code)
        df_fund_quote = pd.read_sql(sql_str1, self.conn)
        df_riskfree = self.get_risk_free(before_trade_date, after_trade_date)
        return df_fund_quote, df_riskfree

    def get_fund_change_pct(self, before_trade_date, after_trade_date, inner_code,):
        if not isinstance(before_trade_date,str):
            before_trade_date = before_trade_date.strftime("%Y-%m-%d")
            after_trade_date = after_trade_date.strftime("%Y-%m-%d")
            
        
        sql_str1 = '''select TradingDay,NVRDailyGrowthRate from 
        MF_FundNetValueRe where TradingDay between '%s' and '%s' and InnerCode
        =%s;''' % (before_trade_date,after_trade_date, inner_code)
        df_fund_quote = pd.read_sql(sql_str1, self.conn)
        df_riskfree = self.get_risk_free(before_trade_date, after_trade_date)
        return df_fund_quote, df_riskfree
    
    def get_risk_free(self,before_trade_date,after_trade_date):
        sql_str2 = "select EndDate,Yield from Bond_CBYieldCurveAll where YearsToMaturity=1 and YieldTypeCode=1 " \
                       "AND StepTypeCode=99 AND CurveCode=10 and EndDate between '%s' and '%s';" % (
                before_trade_date,after_trade_date)
        df_riskfree = pd.read_sql(sql_str2, self.conn)
        return df_riskfree

    def get_fund_daily_change_pct(self, inner_code, start_date):
        sql_str = "select TradingDay,NVRDailyGrowthRate from MF_FundNetValueRe where InnerCode=%s and TradingDay>='%s';" % (
            inner_code, start_date)
        df_fund_daily_quote = pd.read_sql(sql_str, self.conn)
        # df_fund_daily_quote = df_fund_daily_quote.rename(columns={"NVDailyGrowthRate":"NVRDailyGrowthRate"})
        return df_fund_daily_quote

    def get_test_sqlsever(self):
        sql_str = "select A.InnerCode, A.CompanyCode, A.SecuCode, A.SecuAbbr,B.Standard,B.FirstIndustryCode," \
                  "B.FirstIndustryName from SecuMain A inner join LC_ExgIndustry B on " \
                  "A.CompanyCode=B.CompanyCode where A.SecuMarket in (83, 90) and A.SecuCategory = 1 " \
                  "and A.ListedState = 1 and B.Standard=3 and B.CancelDate is null;"
        df_total_stock = pd.read_sql(sql_str, self.conn).set_index("CompanyCode", drop=False)
        self.logger.info(df_total_stock.head())
    
    def format_str(self,code_list=[]):
        if len(code_list)>1:
            result_str = str(tuple(code_list))
        else:
            result_str = "(%s)"%code_list[0]
        return result_str
    
    def get_stock_quote(self, inner_code_list, start_date, end_date):
        sql_str1 = '''select InnerCode,ListedSector from SecuMain where InnerCode 
        in %s;'''%self.format_str(inner_code_list)
        df_info = pd.read_sql(sql_str1, self.conn)
        list_A = df_info[df_info["ListedSector"]!=7]["InnerCode"].tolist()
        list_KCB = df_info[df_info["ListedSector"]==7]["InnerCode"].tolist()
        list_HK = [code for code in inner_code_list if code>=1000001]
        df_stock_quote = pd.DataFrame()
        if list_A:
            sql_str_A = "select InnerCode,TradingDay,ChangePCT from QT_StockPerformance where InnerCode in %s and " \
                      "TradingDay>='%s' and TradingDay<='%s';" % (self.format_str(list_A), start_date, end_date)
            df_stock_quote1 = pd.read_sql(sql_str_A, self.conn)
            df_stock_quote = pd.concat([df_stock_quote,df_stock_quote1],axis=0,sort=True)
            
        if list_KCB:
            sql_str_KCB = "select InnerCode,TradingDay,ChangePCT from LC_STIBPerformance where InnerCode in %s and " \
                      "TradingDay>='%s' and TradingDay<='%s';" % (self.format_str(list_KCB), start_date, end_date)
            df_stock_quote2 = pd.read_sql(sql_str_KCB, self.conn)
            df_stock_quote = pd.concat([df_stock_quote,df_stock_quote2],axis=0,sort=True)
        
        if list_HK:
            sql_str_HK = "select InnerCode,TradingDay,ChangePCT from QT_HKDailyQuote where InnerCode in %s and " \
                      "TradingDay>='%s' and TradingDay<='%s';" % (self.format_str(list_HK), start_date, end_date)
            df_stock_quote3 = pd.read_sql(sql_str_HK, self.conn)
            df_stock_quote = pd.concat([df_stock_quote,df_stock_quote3],axis=0,sort=True) 
        return df_stock_quote

    def get_total_fund(self):
      
        sql_str_new = """select distinct RelatedCode from MF_PortfolioPublished
        where RelatedCode in (select distinct InnerCode from MF_FundArchives 
        where InnerCode in (select distinct InnerCode from MF_JYFundType where 
        Standard=75 and FirstAssetCatCode in (11,12) and CancelDate is null) 
        and IfFOF!=1) and ReportDate>'2012-12-31'"""

        df_fund_innercode = pd.read_sql(sql_str_new, self.conn)
        
        sql_hgs_inner = '''select distinct InnerCode from MF_InvestIndustry
        where InvestType=5 and ReportDate >'2016-01-01';'''
        df_hsg_inner = pd.read_sql(sql_hgs_inner, self.conn)
        df_fund_innercode["Is_HGS"] = [1 if inner in df_hsg_inner["InnerCode"
          ].tolist() else 0 for inner in df_fund_innercode["RelatedCode"
                                                           ].tolist()]
        return df_fund_innercode
    
    def check_hgs(self,inner):
        '''
        检查是否投沪港深的基金
        '''
        sql_hgs_inner = '''select * from MF_InvestIndustry
        where InvestType=5 and ReportDate >'2016-01-01' and InnerCode=%s;'''%inner
        df_hsg_inner = pd.read_sql(sql_hgs_inner, self.conn)
       
        if df_hsg_inner.empty:
            return False
        elif df_hsg_inner.iloc[-1]["InduStandard"]==6:
            return True
        else:
            self.logger.info("当前基金披露的行业标准：%s"%df_hsg_inner.iloc[-1][
                "InduStandard"])
            return False
        
    def get_his_weight(self,inner_code,date_str,flag='ReportDate'):
        if flag=='ReportDate':
            sql_str = '''select InnerCode,StockInnerCode,Weight,InfoPublDate,
            ReportDate from MF_FundPosPenetrate where InnerCode=%s and ReportDate
            ='%s'; '''%(inner_code,date_str)
        else:
            sql_str = '''select InnerCode,StockInnerCode,Weight,InfoPublDate,
            ReportDate from MF_FundPosPenetrate where InnerCode=%s and InfoPublDate
            ='%s'; '''%(inner_code,date_str)
        df = pd.read_sql(sql_str,self.grdb_conn)
        return df
     



if __name__ == "__main__":
    import mylog as mylog_demo
    logger = mylog_demo.set_log()
    GetFundDataDemo = GetFundData(logger=logger)
    # GetFundDataDemo.get_total_fund()
    GetFundDataDemo.get_his_weight(6999,'2021-03-31')
