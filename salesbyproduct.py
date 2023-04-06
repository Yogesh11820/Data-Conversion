import pandas as pd
import json

with open('sales.json') as file:
    data = json.load(file)


df = pd.DataFrame(data['Rows'])

df_c = pd.DataFrame(data['Columns'])

new_df = pd.DataFrame()


for i, item  in enumerate(df['Row']):
     
     row_dict = {}

     if i == 0:
          for j in range(len(item['ColData'])): 
               if(j==0):
                    row_dict[str(df_c['Column'][j]['Value'])] = df['Row'][i]['ColData'][0]['id']
               elif(j==1):
                    row_dict[str(df_c['Column'][j]['Value'])] = df['Row'][i]['ColData'][0]['value']
               else:
                    row_dict[str(df_c['Column'][j]['Value'])] = df['Row'][i]['ColData'][j-1]['value']

     else:
          
         try:
            row_dict[str(df_c['Column'][0]['Value'])] = df['Row'][i]['ColData'][0]['id']
            row_dict[str(df_c['Column'][1]['Value'])] = df['Row'][i]['ColData'][0]['value']
         except:
            pass
               
                
     new_df = new_df.append(row_dict, ignore_index=True)  



print(new_df)
new_df.to_csv("salesbyproduct.csv",index=False)
result = pd.read_csv('salesbyproduct.csv')

print(result)
        
