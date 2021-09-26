# -*- coding: utf-8 -*-
"""
Created on Wed Aug  4 15:26:17 2021

@author: zouhao
"""

# -- coding: utf-8 --
# Author:ZouHao
# email:1084848158@qq.com


import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")


class GetFundDataHGS:
    def __init__(self,GetFundDataDemo):
        self.GetFundDataDemo = GetFundDataDemo
        self.logger = self.GetFundDataDemo.logger
    
    def get_fund_pub_indus_info(self,inner_code, report_date):
        sql_str = '''select InnerCode,InfoPublDate,ReportDate,IndustryName,
        InvestType,RatioInNV,InduStandard,IndustryCode,InduDiscCode,
        MarketValue from MF_InvestIndustry where InnerCode=%s and ReportDate=
        '%s' and IndustryCode not in (1,10001);''' % (inner_code, report_date)
        df = pd.read_sql(sql_str, self.GetFundDataDemo.conn)
        if df.empty:
            return df
        df = self.GetFundDataDemo.adjust_ratioinnv(df)
        return df
    
    def get_fund_poc_info(self,base_info, inner_code, report_date):
        sql_str1 = '''select A.InfoPublDate,A.ReportDate,A.StockInnerCode,
        B.SecuMarket,A.SerialNumber,A.RatioInNV,A.MarketValue,A.InvestType,
        B.CompanyCode,B.SecuCode,B.SecuAbbr,B.ListedSector from 
        MF_KeyStockPortfolio A inner join SecuMain B on A.StockInnerCode=
        B.InnerCode where A.InnerCode=%s and A.ReportDate='%s' order by 
        InfoPublDate desc;'''% (inner_code, report_date)
        
        sql_str2 = '''select A.InfoPublDate,A.ReportDate,A.StockInnerCode,
        B.SecuMarket,A.RatioInNV,A.MarketValue,B.CompanyCode,B.SecuCode,
        B.SecuAbbr, B.ListedSector from MF_KeyStockPortfolio A inner join 
        SecuMain B on A.StockInnerCode=B.InnerCode where A.InnerCode=%s and 
        A.ReportDate='%s' and A.InvestType=3 order by InfoPublDate desc;
        ''' % (inner_code, report_date)
    
                   
        sql_str_hgs = '''select A.InfoPublDate,A.ReportDate,A.StockInnerCode,
        B.SecuMarket,A.RatioInNV,A.SerialNumber,A.MarketValue,A.InvestType,
        B.CompanyCode,B.SecuCode,B.SecuAbbr,B.ListedSector  from 
        MF_KeyStockPortfolio A inner join HK_SecuMain B on A.StockInnerCode=
        B.InnerCode where A.InnerCode=%s and A.ReportDate='%s' order by 
        InfoPublDate desc;''' % (inner_code, report_date)


        if base_info['SecAssetCatCode'] != 1102 or base_info['Type'] == 8:
            df = pd.read_sql(sql_str1, self.GetFundDataDemo.conn)
        else:
            df = pd.read_sql(sql_str2, self.GetFundDataDemo.conn)
            if df.empty:
                df = pd.read_sql(sql_str1, self.GetFundDataDemo.conn)
        df_hgs = pd.read_sql(sql_str_hgs, self.GetFundDataDemo.conn)
        df = pd.concat([df,df_hgs],axis=0,sort=True)
        return df
    
    def combine_industy_classify(self, df, report_date):
        sql_str1 = '''select  InnerCode,CompanyCode,SecuMarket from SecuMain 
        where InnerCode in %s;''' % str(tuple(df['StockInnerCode'].tolist()))
        df_a = pd.read_sql(sql_str1, self.GetFundDataDemo.conn).set_index(
            'CompanyCode', drop=True)
        
        hk_inner = df[df["SecuMarket"]==72]["StockInnerCode"].tolist()
        if hk_inner:
            if len(hk_inner)>1:
                inner_str = str(tuple(hk_inner))
            else:
                inner_str = "(%s)"%hk_inner[0]
            sql_str1_hgs = '''select  InnerCode,CompanyCode,SecuMarket from 
            HK_SecuMain where InnerCode in %s;''' % inner_str
            df_hgs = pd.read_sql(sql_str1_hgs, self.GetFundDataDemo.conn
                                 ).set_index('CompanyCode', drop=True)      
            df1 = pd.concat([df_a,df_hgs],axis=0,sort=True)
            hk_com = df_hgs.index.tolist()
            if len(hk_com)>1:
                hk_com_str = str(tuple(hk_com))
            else:
                hk_com_str = "(%s)"%hk_com[0]
        else:
            df1 = df_a.copy()
        
        df2 = pd.DataFrame()
        if not df_a.empty:
            sql_str2= '''select  CompanyCode,FirstIndustryName,FirstIndustryCode,
            CancelDate,Standard from DZ_ExgIndustry where CompanyCode in %s and 
            Standard=22 and (InfoSource is null or InfoSource!='临时公告') and 
            (CancelDate>'%s' or CancelDate is null)''' % (
            self.GetFundDataDemo.format_str(df_a.index.tolist()),report_date)
        
   
            df2 = pd.read_sql(sql_str2, self.GetFundDataDemo.conn).set_index(
                'CompanyCode', drop=False)
        
        if hk_inner:
            sql_hgs_class = '''select B.CompanyCode,A.IndustryName as 
            FirstIndustryName,A.IndustryCode as FirstIndustryCode, A.CancelDate,
            B.Standard from HK_IndustryCategory A inner join HK_ExgIndustry B 
            on A.IndustryNum=B.IndustryNum and A.Classification=1 and 
            B.CompanyCode in %s and B.Standard=8 and (B.CancelDate 
            is null or B.CancelDate>'%s');''' % (hk_com_str,report_date)
            df_hgs_class = pd.read_sql(sql_hgs_class, self.GetFundDataDemo.conn
                                       ).set_index('CompanyCode', drop=False)
            df2 = pd.concat([df2,df_hgs_class],axis=0,sort=True)
            
        lack_classify_list = df1.loc[list(set(df1.index).difference(set(
            df2.index))), :].index.tolist()
        if lack_classify_list:
            self.logger.error('''heavy poc lack SEC and GiCS classify 
                              CompanyCode!!!''')
            self.logger.error("%s CompanyCode" % lack_classify_list)
            return pd.DataFrame()
        
        use_code = [code for code in df1.index.tolist() if code not in 
                    lack_classify_list]
        
        if len(df1.index.unique())!=len(df1["InnerCode"].unique()):
            inner_lsit = []
            for com_code_num in range(0,df2.shape[0]):
                com_code = df2.index[com_code_num]
                standard = df2.iloc[com_code_num]["Standard"]
                temp_target = df1.loc[com_code]
                if isinstance(temp_target,pd.Series):
                    target_inner = temp_target['InnerCode']
                else:
                    if standard!=22:
                        target_inner = temp_target[temp_target["SecuMarket"
                                        ]==72]["InnerCode" ].iloc[0]
                    else:
                        target_inner = temp_target[temp_target["SecuMarket"
                                        ]!=72]["InnerCode"].iloc[0]
                inner_lsit.append(target_inner)
            df2["StockInnerCode"] = inner_lsit
            df1.rename(columns={"InnerCode": "StockInnerCode"}, inplace=True)
            df1.set_index("StockInnerCode",inplace=True)
            df2.set_index("StockInnerCode",inplace=True)
            use_df = pd.concat([df1,df2],sort=True, axis=1)
            use_df["StockInnerCode"] = use_df.index
        else:
            use_df = pd.concat([df2, df1.loc[use_code, :]], sort=True, axis=1)
            use_df.rename(columns={"InnerCode": "StockInnerCode"}, inplace=True)
            use_df = use_df.set_index('StockInnerCode', drop=False)
        df_result = self.GetFundDataDemo.handle_industry_change(df, use_df)

        return df_result
    
    def get_stock_bool_info_new(self,before_trade_date,info_pub_date,
                                heav_code=[]):
        before_trade_str =  before_trade_date.strftime("%Y-%m-%d")
        
        sql_str='''select C.InnerCode,C.CompanyCode,C.SecuCode,C.ChiNameAbbr as 
        SecuAbbr,C.SecuCategory,D.CancelDate,D.IndustryName,D.IndustryCode,
        C.HKSTMark,C.SecuCategory from HK_SecuCodeTable C inner join (select 
        A.CompanyCode,A.CancelDate,B.IndustryName,B.IndustryCode from 
        HK_ExgIndustry A inner join HK_IndustryCategory B on A.IndustryNum=
        B.IndustryNum where A.Standard=8 and B.Classification=1 and 
        (B.CancelDate<='%s' or B.CancelDate is null)) D on C.CompanyCode=
        D.CompanyCode where C.ListedDate<='%s';'''%(before_trade_str,
        before_trade_str)   
        
        df_hgs_stock = pd.read_sql(sql_str, self.GetFundDataDemo.conn)
        df_hgs_stock1 = df_hgs_stock[df_hgs_stock["HKSTMark"]=="是"].drop(
            ["HKSTMark","SecuCategory"],axis=1)
        df_hgs_stock1['StockInnerCode'] = df_hgs_stock1["InnerCode"]
        df_hgs_stock1.set_index("StockInnerCode", inplace=True)
        
        # classify_se = df_hgs_stock1["InnerCode"].value_counts()

        df_total_stock2 = df_hgs_stock1[
            (df_hgs_stock1['CancelDate'] > pd.to_datetime(info_pub_date
             ).strftime("%Y-%m-%d")) | np.isnan( df_hgs_stock1['CancelDate'])]
  
        
        temp_classify = df_total_stock2['InnerCode'].value_counts()
        df_result = df_total_stock2.loc[temp_classify[temp_classify == 1].index]
        much_classify_df = df_total_stock2.loc[temp_classify[temp_classify > 1
                                                             ].index]
        if not much_classify_df.empty:
            much_df_list = [stock_df[stock_df['CancelDate'] == stock_df[
                'CancelDate'].min()] for innner_code, stock_df in 
                much_classify_df.groupby("InnerCode")]
            df_result = pd.concat([df_result, pd.concat(much_df_list, sort=True,
                                                    axis=0)], axis=0, sort=True)     
        return df_result
    
