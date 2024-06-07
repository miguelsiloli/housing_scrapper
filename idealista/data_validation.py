import pandera as pa
from pandera import Column, DataFrameSchema, Check
import pandas as pd
import pandas as np

# Define the schema
schema = DataFrameSchema({
    "title": Column(str, 
                    nullable=False),  # Assuming title can be nullable
    "link": Column(str, 
                   checks=[
        Check(lambda x: x.str.startswith("https://www.idealista.pt/imovel/"), 
              error="Invalid URL format")
    ], unique=True),
    "description": Column(str, 
                          nullable=True),  # Assuming description can be nullable
    "garage": Column(bool),
    "price": Column('int32', 
                    Check.greater_than(0)),
    "home_size": Column(str, 
                        checks=[
        Check(lambda x: x.str.match(r"T\d+"), 
              error="Invalid home size format")
    ]),
    "home_area": Column('int32', 
                        Check.greater_than(0)),
    "floor": Column('int32'),  # Assuming floor can be negative (basement levels)
    "elevator": Column(bool),
    "price_per_sqr_meter": Column('float32', 
                                  Check.greater_than(0)),
    "date": Column('datetime64[ns]', 
                   checks=[
        Check(lambda x: pd.to_datetime(x, 
                                       format='%d-%m-%Y', 
                                       errors='coerce').notna(), 
              error="Invalid date format")
    ])
})
