import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean

def detect_column_type(series):
    """Automatically detect the best SQLAlchemy type for a pandas series"""
    if pd.api.types.is_integer_dtype(series):
        return Integer
    elif pd.api.types.is_float_dtype(series):
        return Float
    elif pd.api.types.is_bool_dtype(series):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(series):
        return DateTime
    else:
        # Check if string could be a date
        try:
            pd.to_datetime(series)
            return DateTime
        except:
            return String

def generate_database_code(csv_files):
    """Generate complete database setup code from CSV files"""
    code = """from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
import pandas as pd

def setup_database():
    engine = create_engine('sqlite:///ecommerce.db')
    metadata = MetaData()
    
"""
    table_definitions = []
    load_commands = []
    
    for csv_file in csv_files:
        # Read CSV to analyze structure
        df = pd.read_csv(csv_file)
        
        # Generate table name from filename
        table_name = os.path.splitext(csv_file)[0].lower().replace(' ', '_').replace('-', '_')
        
        # Generate columns
        columns = []
        for col in df.columns:
            col_type = detect_column_type(df[col])
            columns.append(f"    Column('{col}', {col_type.__name__})")
        
        # Add table definition
        table_def = f"    {table_name} = Table('{table_name}', metadata,\n"
        table_def += ',\n'.join(columns)
        table_def += "\n    )\n"
        table_definitions.append(table_def)
        
        # Add load command
        load_commands.append(f"    pd.read_csv('{csv_file}').to_sql('{table_name}', engine, if_exists='replace', index=False)\n")
    
    # Combine all parts
    code += '\n'.join(table_definitions)
    code += "\n    # Create tables\n    metadata.create_all(engine)\n\n"
    code += "    # Load data\n"
    code += ''.join(load_commands)
    code += "\n    return engine\n"
    
    return code

if __name__ == "__main__":
    # Get all CSV files in current directory
    csv_files = [f for f in os.listdir() if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in current directory!")
    else:
        print(f"Found {len(csv_files)} CSV files: {', '.join(csv_files)}")
        db_code = generate_database_code(csv_files)
        
        with open("generated_database.py", "w") as f:
            f.write(db_code)
        
        print("Database code generated in 'generated_database.py'")
        print("\nTo use it:")
        print("1. Import the setup_database function")
        print("2. Call engine = setup_database()")
        print("3. Use the engine for SQL operations")
