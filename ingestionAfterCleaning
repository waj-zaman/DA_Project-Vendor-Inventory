import duckdb
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename = 'log/get_vendor_summary.log',
    level = logging.DEBUG,
    formt = '%(asctime)s - %(levelname)s - %(message)s',
    filemode = 'a'
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
    ),
    
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Description,
            p.Brand,
            p.PurchasePrice,
            pp.Price as ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchasedQuantity,
            SUM(p.Dollars) AS TotalPurchasedDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY vendorNo, Brand
    )
    
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchasedQuantity,
        ps.TotalPurchasedDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary as ps
    LEFT JOIN SalesSummary as ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchasedDollars DESC
    
    """, conn)

    return vendor_sales_summary


def clean_data(df): 
    '''This function will clean the data'''

    # Changing data type to float
    df['Vendorname'] = df['VendorName'].astype('float')

    # Filling missing values
    df.fillna(0, inplace=True)

    # Removing spaces from the categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating some new columns for better analysis
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchasedDollars']
    vendor_sales_summary['ProfitMargin'] = ( vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars'] ) * 100
    vendor_sales_summary['StockTurnOver'] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchasedQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchasedDollars']

    return df

if __name__ == '__main__':
    # Creating database connection
    conn = duckdb.connect('vendor_inventory.duckdb')

    logging.info('Creating Vendor Summary Table... ')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting Data...')
    ingest_db = ('vendor_sales_summary', clean_df, conn)
    logging.info('Ingestion Completed')
    



