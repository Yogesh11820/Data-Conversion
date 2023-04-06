import pandas as pd 
import json


def fetch_data(data,new_df):
   if isinstance(data,dict):
      for key,value in data.items():
         if key=='ColData':
            row_dict = {}
            if 'id' in value[0]:
                  row_dict['id'] = str(value[0]['id'])
                  row_dict[str(df_c['Column'][0]['ColTitle'])] = str(value[0]['value'])
                  row_dict[str(df_c['Column'][1]['ColTitle'])] = str(value[1]['value'])  

            else:
                  row_dict[str(df_c['Column'][0]['ColTitle'])] = str(value[0]['value'])
                  row_dict[str(df_c['Column'][1]['ColTitle'])] = str(value[1]['value'])

            new_df = new_df.append(row_dict,ignore_index=True)  

         else:
             new_df = fetch_data(value,new_df)   

   elif isinstance(data,list):
       for item in data:
           new_df = fetch_data(item,new_df)
    
   return new_df

with open('newfile.json') as file:
   data =  json.load(file)


df_c = pd.DataFrame(data['Columns'])



new_df = pd.DataFrame()


new_df = fetch_data(data,new_df)

print(new_df)

new_df.to_csv("Balancesheetfile.csv",index=False)

result = pd.read_csv("Balancesheetfile.csv")

print(result)

