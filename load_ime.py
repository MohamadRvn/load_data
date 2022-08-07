import pandas as pd
import jdatetime as jdt

def loadime(current_month):
      '''
      Load ime files in ime history directory into a single df
      '''

      history_list=[str(x) for x in range(139701,139713)]+[str(x) for x in range(139801,139813)]+[str(x) for x in range(139901,139913)]+[str(x) for x in range(140001,current_month+1)]

      ime_dict={}

      def get_year_string(date_string):
          return int(date_string[:4])

      def get_month_string(date_string):
          return int(date_string[5:7])

      def get_day_string(date_string):
          return int(date_string[8:])

      for key in history_list:
          ime_dict[key]=pd.read_csv("csv directory"%key)
          columns_ime_dict_key=list(ime_dict[key].columns)
          new_coluns_ime_dict_key=["name","symbol","producer","price_close","supplied(mt)","price_base","demand(mt)","vol(mt)","date_transaction","date_delivery"]
          ime_dict[key].rename(columns=dict(zip(columns_ime_dict_key,new_coluns_ime_dict_key)),inplace=True)
          ime_dict[key]["year_trans"]=ime_dict[key]["date_transaction"].apply(get_year_string)
          ime_dict[key]["month_trans"]=ime_dict[key]["date_transaction"].apply(get_month_string)
          ime_dict[key]["day_trans"]=ime_dict[key]["date_transaction"].apply(get_day_string)
          ime_dict[key]["date_t"]=ime_dict[key].apply(lambda x: jdt.date(x["year_trans"],x["month_trans"],x["day_trans"]).togregorian(),axis=1)
          ime_dict[key]["year_delivery"]=ime_dict[key]["date_delivery"].apply(get_year_string)
          ime_dict[key]["month_delivery"]=ime_dict[key]["date_delivery"].apply(get_month_string)
          ime_dict[key]["day_delivery"]=ime_dict[key]["date_delivery"].apply(get_day_string)
          ime_dict[key]["date_d"]=ime_dict[key].apply(lambda x: jdt.date(x["year_delivery"],x["month_delivery"],x["day_delivery"]).togregorian(),axis=1)
          ime_dict[key].drop(columns=["date_transaction","date_delivery","year_trans","year_delivery","month_trans","month_delivery","day_trans","day_delivery"],inplace=True)
          ime_dict[key]=ime_dict[key].fillna(0)

      ime_df=pd.DataFrame()
      for key in ime_dict.keys():
          ime_df=ime_df.append(ime_dict[key])

      ime_df.set_index(["producer","name"],inplace=True)
      ime_df=ime_df.sort_index()

      return ime_df
