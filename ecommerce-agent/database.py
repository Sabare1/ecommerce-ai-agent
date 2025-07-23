import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
import pandas as pd


def setup_database():
    # Get absolute path to the database
    db_path = os.path.join(os.path.dirname(__file__), 'ecommerce.db')
    engine = create_engine(f'sqlite:///{db_path}')
    metadata = MetaData()

    # Define tables with simplified names
    ad_metrics = Table('ad_metrics', metadata,
                       Column('date', String),  # Using String instead of DateTime for simplicity
                       Column('item_id', Integer),
                       Column('ad_sales', Float),
                       Column('impressions', Integer),
                       Column('ad_spend', Float),
                       Column('clicks', Integer),
                       Column('units_sold', Integer)
                       )

    product_eligibility = Table('product_eligibility', metadata,
                                Column('eligibility_datetime_utc', String),
                                Column('item_id', Integer),
                                Column('eligibility', Boolean),
                                Column('message', String)
                                )

    sales_metrics = Table('sales_metrics', metadata,
                          Column('date', String),
                          Column('item_id', Integer),
                          Column('total_sales', Float),
                          Column('total_units_ordered', Integer)
                          )

    # Create tables
    metadata.create_all(engine)

    # Load data with proper file paths
    data_dir = os.path.join(os.path.dirname(__file__), 'data')

    try:
        # Load ad metrics
        ad_metrics_path = os.path.join(data_dir, 'Product-Level Ad Sales and Metrics (mapped).csv')
        pd.read_csv(ad_metrics_path).to_sql(
            'ad_metrics',
            engine,
            if_exists='replace',
            index=False
        )

        # Load eligibility data
        eligibility_path = os.path.join(data_dir, 'Product-Level Eligibility Table (mapped).csv')
        pd.read_csv(eligibility_path).to_sql(
            'product_eligibility',
            engine,
            if_exists='replace',
            index=False
        )

        # Load sales metrics
        sales_metrics_path = os.path.join(data_dir, 'Product-Level Total Sales and Metrics (mapped).csv')
        pd.read_csv(sales_metrics_path).to_sql(
            'sales_metrics',
            engine,
            if_exists='replace',
            index=False
        )

    except Exception as e:
        print(f"Error loading data: {e}")
        raise

    return engine