import pandas as pd

# Load the dataset
# Ensure the file name matches your local file path
file_path = 'Grocery_Inventory_and_Sales_Dataset.csv'
df = pd.read_csv(file_path)

# --- Basic Preprocessing ---

# 1. Clean 'Unit_Price' column
# Remove '$' and ',' then convert to float for calculations
df['Unit_Price'] = df['Unit_Price'].astype(str).str.replace('$', '').str.replace(',', '').astype(float)

# 2. Convert 'Expiration_Date' to datetime objects
# errors='coerce' turns unparseable dates into NaT (Not a Time) so we can easily filter them
df['Expiration_Date'] = pd.to_datetime(df['Expiration_Date'], errors='coerce')

# --- Filtering ---

# 3. Remove all 'Discontinued' products
df_clean = df[df['Status'] != 'Discontinued']

# 4. Remove all products with no expiry dates (where date is NaT/Null)
df_clean = df_clean.dropna(subset=['Expiration_Date'])

# --- Column Selection ---

# 5. Keep only the specified columns
columns_to_keep = [
    'Product_Name', 
    'Product_ID', 
    'Expiration_Date', 
    'Stock_Quantity', 
    'Supplier_Name', 
    'Unit_Price'
]
df_clean = df_clean[columns_to_keep]

# Reset index for a clean dataframe
df_clean = df_clean.reset_index(drop=True)

# Display the first few rows of the cleaned data
print("Cleaned Data Preview:")
print(df_clean.head())

# Save the processed data to a new CSV file
df_clean.to_csv('Processed_Grocery_Data.csv', index=False)