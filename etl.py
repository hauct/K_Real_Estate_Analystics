import pandas as pd
import sqldf

# Read and clean data
def read_and_clean(name_csv):
    df = pd.read_csv(name_csv)
    if name_csv == 'data/sale.csv':
        ## Convert to datetime on time columns
        df['date_acquired'] = pd.to_datetime(df['date_acquired'])
        df['date_sold'] = pd.to_datetime(df['date_sold'])
        ## Count days on market
        df['days_on_market'] = (df['date_sold'] - df['date_acquired']).dt.days

    if name_csv == 'data/mkt.csv':
        ## Convert to datetime on time columns
        df['date_of_disbursement'] = pd.to_datetime(df['date_of_disbursement'])
    return df

# Function to aggregate marketing data
def calculate_mkt_agg():
    mkt_agg_query = '''
    SELECT date_of_disbursement, expendtiture_category, SUM(cost) AS cost
    FROM mkt AS m
    GROUP BY date_of_disbursement, expendtiture_category
    ORDER BY date_of_disbursement
    '''
    mkt_agg = sqldf.run(mkt_agg_query)
    return mkt_agg

# Function to calculate total cost for each inventory ID
def calculate_invertory_id_cost():
    inventory_id_cost_query = '''
    SELECT inventory_id, SUM(cost) AS cost
    FROM mkt AS m
    GROUP BY inventory_id
    '''
    inventory_id_cost = sqldf.run(inventory_id_cost_query)
    return inventory_id_cost

# Function to calculate sales data for each inventory ID
def calculate_sale_agg_1():
    sale_agg_1_query = '''
        SELECT s.inventory_id, s.type_of_property, s.date_acquired, s.date_sold, s.days_on_market, COALESCE(i.cost, 0) AS cost, s.selling_price - (s.buying_price + COALESCE(i.cost, 0)) AS profit
        FROM sale AS s
        LEFT JOIN inventory_id_cost AS i ON s.inventory_id = i.inventory_id
    '''
    sale_agg_1 = sqldf.run(sale_agg_1_query)
    return sale_agg_1

# Function to aggregate sales data
def calculate_sale_agg_2():
    sale_agg_2_query = '''
    SELECT date_sold, type_of_property, COUNT(inventory_id) AS num_in, AVG(days_on_market) AS avg_days_on_market, SUM(profit) AS profit
    FROM sale_agg_1
    WHERE date_sold IS NOT NULL
    GROUP BY date_sold, type_of_property
    ORDER BY date_sold, type_of_property
    '''
    sale_agg_2 = sqldf.run(sale_agg_2_query)
    return sale_agg_2

# Function to find the top unsold inventory IDs
def calculate_top_unsold_invetory_id():
    top_unsold_invetory_id_query = '''
    SELECT s.inventory_id, s.type_of_property, s.date_acquired, i.cost, CURRENT_DATE AS current_date
    FROM sale AS s
    LEFT JOIN inventory_id_cost AS i ON s.inventory_id = i.inventory_id
    WHERE s.date_sold IS NULL
    ORDER BY cost DESC
    '''
    top_unsold_invetory_id = sqldf.run(top_unsold_invetory_id_query)
    return top_unsold_invetory_id

# Function to export dataframe to CSV
def export_to_csv(df, filename):
    df.to_csv(filename, index=False)

if __name__ == '__main__':
    print('='*20)
    mkt  = read_and_clean('data/mkt.csv')
    sale = read_and_clean('data/sale.csv')
    print('Reading files: OK')

    print('='*20)
    mkt_agg = calculate_mkt_agg()
    inventory_id_cost = calculate_invertory_id_cost()
    sale_agg_1 = calculate_sale_agg_1()
    sale_agg = calculate_sale_agg_2()
    top_unsold_invetory_id = calculate_top_unsold_invetory_id()
    print('Calculating tables: OK')

    print('='*20)
    export_to_csv(mkt_agg, 'data/mkt_agg.csv')
    export_to_csv(sale_agg, 'data/sale_agg.csv')
    export_to_csv(top_unsold_invetory_id, 'data/top_unsold_invetory_id.csv')
    print('Exporting files: OK')
    print('Done')