import pandas as pd
from sqlalchemy import create_engine

# Sample data
db = {
    'ConsumerID': [101, 102, 103],
    'Name': ['Neeraj Shilwant', 'Shreyash Rajgire', 'Bandya Mama'],
    'Address': ['Manjula Masulkar , Pune , Maharashtra, 411017', '101 Street , Pune, Maharashtra, 411017', '506 street , Pune, Maharashtra, 411017'],
    'ContactNumber': ['4343432423', '4345678903', '1234567890'],
    'EmailAddress': ['neeraj@example.com', 'shreyash@example.com', 'bandya@example.com'],
    'AccountNumber': ['ABC123', 'XYZ456', 'ZYX789'],
    'MeterNumber': ['M0012345', 'M0021234', 'M003000'],
    'TariffPlan': ['Plan1', 'Plan2', 'Plan'],
    'ConsumptionHistory': ['History1', 'History2', 'History3'],
    'PaymentStatus': ['Paid', 'Pending', 'Paid']
}

# Create DataFrame
sample_data = pd.DataFrame(db)

# Define database connection strings
source_db_connection_string = 'sqlite:///source.db'
target_db_connection_string = 'sqlite:///target.db'

# Connect to source database
source_engine = create_engine(source_db_connection_string)

# Connect to target database
target_engine = create_engine(target_db_connection_string)

# Load sample data into source database
sample_data.to_sql('consumer_data_table', source_engine, if_exists='replace', index=False)

# Define SQL query to extract consumer data
query = """
    SELECT 
        ConsumerID,
        Name,
        Address,
        ContactNumber,
        EmailAddress,
        AccountNumber,
        MeterNumber,
        TariffPlan,
        ConsumptionHistory,
        PaymentStatus
    FROM 
        consumer_data_table
"""

# Extract data from source database
consumer_data = pd.read_sql(query, source_engine)

# Perform data transformation and mapping
consumer_data[['First Name', 'Last Name']] = consumer_data['Name'].str.split(' ', 1, expand=True)
#consumer_data[['Address Line 1', 'Address Line 2', 'City', 'State', 'Zip Code']] = 
address = consumer_data['Address'].str.split(',', expand=True)
addline = address[0].str.split(' ', expand =True)
consumer_data["Address Line 1"] = addline[0]
consumer_data["Address Line 2"] = addline[1] 
consumer_data["City"] = address[1]
consumer_data["State"] = address[2]
consumer_data["ZipCode"] = address[3]



consumer_data.rename(columns={'ContactNumber': 'Phone Number', 'EmailAddress': 'Email Address'}, inplace=True)

# Drop columns that are no longer needed
consumer_data.drop(['Name', 'Address', 'AccountNumber', 'MeterNumber', 'TariffPlan', 'ConsumptionHistory','PaymentStatus'], axis=1, inplace=True)
newData = consumer_data[['ConsumerID', 'First Name', 'Last Name', 'Address Line 1', 'Address Line 2', 'City', 'State', 'ZipCode', 'Phone Number', 'Email Address']]
# Load mapped data into target database
newData.to_sql('SMART360_Consumer', target_engine, if_exists='replace', index=False)

# Validate by reading from target database and displaying the data
validated_data = pd.read_sql('SELECT * FROM SMART360_Consumer', target_engine)
print(validated_data)