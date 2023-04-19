from flask import Flask, jsonify ,render_template
from pyspark.sql import SparkSession
from app import app
import pandas as pd
from pyspark.sql.functions import col,month, desc
import datetime

spark = SparkSession.builder.appName('TopSales').getOrCreate()


@app.route('/top_sales_price')
def top_sales_price():

   
     df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv', header=True, inferSchema=True)
     #new column Total Sales  
     df_spark = df_spark.withColumn('Total Sales',df_spark['Quantity Ordered']*df_spark['Selling Price'])

     

     df_spark = df_spark.filter(df_spark['Order Date'].contains('-04-'))
     df_april_top_sales = df_spark.groupby('Product').agg({'Total Sales' : 'sum'}).orderBy('sum(Total Sales)',ascending=False).limit(5)
     df_april_pd = df_april_top_sales.toPandas() 

      
     response1 = jsonify(df_april_pd.to_dict(orient='records'))    # april Month

     response1 = df_april_pd.to_dict(orient='records')
     #May month sales
     df_may = df_spark.filter(df_spark['Order Date'].contains('05-'))
     df_may = df_may.withColumn('Total Sales',df_may['Quantity Ordered']*df_may['Selling Price'])
     df_may_top_sales = df_may.groupby('Product').agg({'Total Sales':'sum'}).orderBy('sum(Total Sales)',ascending=False).limit(5)
     df_may_pd = df_may_top_sales.toPandas()
    
     response2 = df_may_pd.to_dict(orient='records')

     
     return render_template('top_sales_value.html', response1=response1, response2=response2)


    

@app.route('/top_sales_quantity')
def top_sales_quantity():

    #df = pd.read_excel(r'C:\Users\yoges\OneDrive\Desktop\Sales_April_2019_U.csv')
    

    df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv', header=True, inferSchema=True)
    
    #april month sales
    df_april = df_spark.filter(df_spark['Order Date'].contains('04-'))
    df_april = df_april.withColumn('Total Sales',df_april['Quantity Ordered'])
    df_april_top_sales = df_april.groupby('Product').agg({'Total Sales' : 'sum'}).orderBy('sum(Total Sales)',ascending=False).limit(5)
    df_april_pd = df_april_top_sales.toPandas()
    response1 = df_april_pd.to_dict(orient='records')

    #may month sales
    df_may = df_spark.filter(df_spark['Order Date'].contains('05-'))
    df_may = df_may.withColumn('Total Sales',df_may['Quantity Ordered'])
    df_may_top_sales = df_may.groupby('Product').agg({'Total Sales' : 'sum'}).orderBy('sum(Total Sales)',ascending=False).limit(5)
    df_may_pd = df_may_top_sales.toPandas()
    response2 = df_may_pd.to_dict(orient='records')

    return render_template('top_sales_quan.html',response1=response1,response2=response2)




@app.route('/sales_outstanding')
def sales_outstanding():
    ''' DSO = (Accounts Receivable(at purchase time) / Total Credit Sales) x Number of Days in the Period '''
    ''' at purchase time 50% amount paid & and remaining 50% after 7 days'''

    df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv',header=True,inferSchema=True)

    df_spark = df_spark.withColumn('Total product valuation', df_spark['Selling Price']*df_spark['Quantity Ordered'])
    df_totalvaluation = df_spark.agg({'Total product valuation':'sum'})    # total
    amount = round(df_totalvaluation.collect()[0][0],0) 
    print(amount)
    Account_Receivable = amount/2
    Total_Sale = amount
    No_of_days = 7

    DSO = (Account_Receivable/Total_Sale)*No_of_days
    
    return f"The Days Sales Outstanding is  - {DSO}"
   

@app.route('/revenue')
def revenue():
    '''no of unit sold*avg sales price per unit''' 

    df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv',header=True,inferSchema=True)
    
    
    df_spark = df_spark.withColumn('Total product valuation', df_spark['Selling Price']*df_spark['Quantity Ordered'])
    df_totalvaluation = df_spark.agg({'Total product valuation':'sum'})    # total
    amount = round(df_totalvaluation.collect()[0][0],0) 
    print(amount)
    #return f"The revenue of company in 2019 is ${amount}"

    df_april = df_spark.filter(df_spark['Order Date'].contains('04'))
    df_april = df_april.withColumn('Total Valuation',df_april['Quantity Ordered']*df_april['Selling Price'])
    df_totalvalutaion = df_april.agg({'Total Valuation':'sum'})
    april_revenue = round(df_totalvalutaion.collect()[0][0],0)
    print(april_revenue)
    #return f'The Revenue of company in April 2019 is ${april_revenue}'

    df_may = df_spark.filter(df_spark['Order Date'].contains('05'))
    df_may = df_may.withColumn('Total Valuation',df_may['Quantity Ordered']*df_may['Selling Price'])
    df_totalvalutaion = df_may.agg({'Total Valuation':'sum'})
    may_revenue = round(df_totalvalutaion.collect()[0][0],0)
    print(may_revenue)

    return f'The Revenue of company in April 2019 is ${april_revenue} \n The Revenue of company in may 2019 is ${may_revenue} \n {april_revenue+may_revenue}'
    


@app.route('/cagr')
def cagr():
    '''  BV(Beginning Value) --> 1000000 EV(Ending Value) --> 2839089 '''

    df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv',header=True,inferSchema=True)
 
    df_spark = df_spark.withColumn('Total product valuation', df_spark['Selling Price']*df_spark['Quantity Ordered'])
    df_totalvaluation = df_spark.agg({'Total product valuation':'sum'})    # total
    amount = round(df_totalvaluation.collect()[0][0],0) 
    BV = 1000000
    n = 1
    ratio = pow((amount+BV)/BV,1/n)
    CAGR = (ratio-1)*100
    print(CAGR)
    return f"The CAGR of Company for April Month -  {CAGR}"


@app.route('/debtors_ageing')
def debtors_ageing():
    df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv', header=True, inferSchema=True)
    
    df_spark = df_spark.withColumn('Total Sales', df_spark['Quantity Ordered'] * df_spark['Selling Price'])
    
    df_product_sales = df_spark.groupby('Product').agg({'Total Sales':'sum'})
    # df_product_sales.printSchema()
    df_product_sales = df_product_sales.withColumn('Current', col('sum(Total Sales)')/2)
    df_product_sales = df_product_sales.withColumn('1-30 Days', col('sum(Total Sales)')/2)

    df_pandas  = df_product_sales.toPandas()
    
    print(df_pandas)
    
    html = df_pandas.to_html()
    file = open('templates/debtors_ageing.html','w')
    file.write(html)
    file.close()
    return render_template('debtors_ageing.html')
    
    
@app.route("/dt")
def dt():

        def convert_date(date_str):
              try:
                    date_obj = datetime.datetime.strptime(date_str, '%m-%d-%Y %H:%M')
              except ValueError:
                    date_obj = datetime.datetime.strptime(date_str, '%m/%d/%y %H:%M')
              return date_obj

        df = pd.read_csv('Sales_April_updated.csv')

        df['Order Date'] = df['Order Date'].apply(convert_date)

        print(df.head(10))

        df.to_csv('Sales_April_updated.csv', index=False)


        return "Great success"

        


        #orders_by_month = data.groupby(pd.Grouper(key='Order Date', freq='M')).sum()
        #April month sales 
        #df['Order Date'].dt.month
        #  response2 = jsonify(df_may_pd.to_dict(orient='records'))
        #  print(response2)


@app.route('/new_code')
def new_code():

    df_spark = spark.read.csv(r'C:\Users\yoges\OneDrive\Desktop\intern\Pyspark-app\Sales_April_updated.csv', header=True, inferSchema=True)
    df_spark = df_spark.withColumn('Total Sales', df_spark['Quantity Ordered'] * df_spark['Selling Price'])

   
    df_monthly_sales = df_spark.groupBy([month('Order Date').alias('Month'), 'Product']).agg({'Total Sales': 'sum'})

    df_monthly_sales = df_monthly_sales.orderBy(['Month', desc('sum(Total Sales)')]).limit(30)
    
    response = jsonify(df_monthly_sales.toPandas().to_dict(orient='records'))
    return response

   