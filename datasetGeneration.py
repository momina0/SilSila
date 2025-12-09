import numpy as np
import pandas as pd
import random
import os
from datetime import datetime, timedelta

# ----------------------------
# CONFIGURATION
# ----------------------------
NUM_CLIENTS = 10
NUM_PRODUCTS = 200
BATCHES_PER_PRODUCT_PER_CLIENT = 3
SALES_DAYS = 240

np.random.seed(42)
random.seed(42)

# Create output folder
os.makedirs("data", exist_ok=True)

today = datetime.now().date()

# ----------------------------
# COMPREHENSIVE REALISTIC PAKISTANI FMCG PRODUCTS
# Based on actual market products from Nestle, Unilever, P&G, and local brands
# ----------------------------
pakistani_products_base = [
    # DAIRY PRODUCTS - Major brands in Pakistan
    {"brand": "Nestle", "product": "Everyday Milk", "category": "Dairy", "variants": ["1L", "500ml", "250ml"], "mrp_range": (180, 280), "shelf_life": (14, 21)},
    {"brand": "Olpers", "product": "Full Cream Milk", "category": "Dairy", "variants": ["1L", "500ml"], "mrp_range": (200, 300), "shelf_life": (14, 21)},
    {"brand": "Haleeb", "product": "Fresh Milk", "category": "Dairy", "variants": ["1L", "500ml"], "mrp_range": (190, 290), "shelf_life": (14, 21)},
    {"brand": "Nurpur", "product": "Milk", "category": "Dairy", "variants": ["1L", "500ml"], "mrp_range": (185, 275), "shelf_life": (14, 21)},
    {"brand": "Dairy Omung", "product": "Milk", "category": "Dairy", "variants": ["1L"], "mrp_range": (180, 220), "shelf_life": (14, 21)},
    {"brand": "Nestle", "product": "Yogurt", "category": "Dairy", "variants": ["400g", "900g"], "mrp_range": (80, 180), "shelf_life": (21, 30)},
    {"brand": "Tarang", "product": "Dahi", "category": "Dairy", "variants": ["500g", "1kg"], "mrp_range": (100, 200), "shelf_life": (15, 25)},
    {"brand": "Olpers", "product": "Cheese Slices", "category": "Dairy", "variants": ["200g", "400g"], "mrp_range": (250, 450), "shelf_life": (90, 120)},
    {"brand": "Nurpur", "product": "Butter", "category": "Dairy", "variants": ["200g"], "mrp_range": (280, 350), "shelf_life": (90, 180)},
    
    # BEVERAGES - Carbonated, Juices, Water
    {"brand": "Coca Cola", "product": "Coke", "category": "Beverages", "variants": ["250ml", "500ml", "1.5L", "2.25L"], "mrp_range": (50, 200), "shelf_life": (180, 365)},
    {"brand": "Pepsi", "product": "Pepsi", "category": "Beverages", "variants": ["250ml", "500ml", "1.5L", "2.25L"], "mrp_range": (50, 195), "shelf_life": (180, 365)},
    {"brand": "7UP", "product": "7UP", "category": "Beverages", "variants": ["250ml", "500ml", "1.5L"], "mrp_range": (50, 180), "shelf_life": (180, 365)},
    {"brand": "Sprite", "product": "Sprite", "category": "Beverages", "variants": ["250ml", "500ml", "1.5L"], "mrp_range": (50, 180), "shelf_life": (180, 365)},
    {"brand": "Fanta", "product": "Orange", "category": "Beverages", "variants": ["500ml", "1.5L"], "mrp_range": (70, 170), "shelf_life": (180, 365)},
    {"brand": "Shezan", "product": "Mango Juice", "category": "Beverages", "variants": ["250ml", "1L"], "mrp_range": (60, 200), "shelf_life": (180, 270)},
    {"brand": "Shezan", "product": "Apple Juice", "category": "Beverages", "variants": ["1L"], "mrp_range": (180, 220), "shelf_life": (180, 270)},
    {"brand": "Nestle", "product": "Pure Life Water", "category": "Beverages", "variants": ["500ml", "1.5L"], "mrp_range": (30, 80), "shelf_life": (365, 730)},
    {"brand": "Aquafina", "product": "Water", "category": "Beverages", "variants": ["500ml", "1.5L"], "mrp_range": (30, 75), "shelf_life": (365, 730)},
    {"brand": "Gourmet", "product": "Green Tea", "category": "Beverages", "variants": ["25 bags", "50 bags", "100 bags"], "mrp_range": (150, 450), "shelf_life": (365, 730)},
    {"brand": "Lipton", "product": "Yellow Label Tea", "category": "Beverages", "variants": ["100 bags", "200 bags"], "mrp_range": (250, 550), "shelf_life": (365, 730)},
    {"brand": "Vital", "product": "Energy Drink", "category": "Beverages", "variants": ["250ml"], "mrp_range": (100, 130), "shelf_life": (365, 540)},
    
    # SNACKS - Chips, Biscuits, Crackers
    {"brand": "Lays", "product": "Masala Chips", "category": "Snacks", "variants": ["34g", "68g", "90g"], "mrp_range": (30, 120), "shelf_life": (90, 180)},
    {"brand": "Lays", "product": "Wavy Chips", "category": "Snacks", "variants": ["68g"], "mrp_range": (80, 100), "shelf_life": (90, 180)},
    {"brand": "Kolson", "product": "Slanty", "category": "Snacks", "variants": ["18g", "40g"], "mrp_range": (20, 60), "shelf_life": (90, 180)},
    {"brand": "Super Crisp", "product": "Nimko", "category": "Snacks", "variants": ["100g", "200g"], "mrp_range": (50, 120), "shelf_life": (60, 120)},
    {"brand": "Cheetos", "product": "Crunchy", "category": "Snacks", "variants": ["48g"], "mrp_range": (50, 70), "shelf_life": (90, 180)},
    {"brand": "Kurkure", "product": "Chatpata", "category": "Snacks", "variants": ["62g", "110g"], "mrp_range": (40, 100), "shelf_life": (90, 180)},
    {"brand": "Peek Freans", "product": "Sooper Biscuit", "category": "Snacks", "variants": ["Family Pack", "Half Roll"], "mrp_range": (40, 140), "shelf_life": (120, 180)},
    {"brand": "Peek Freans", "product": "Chocolate Chip", "category": "Snacks", "variants": ["90g"], "mrp_range": (60, 80), "shelf_life": (120, 180)},
    {"brand": "Jubilee", "product": "Chocolate Biscuit", "category": "Snacks", "variants": ["90g"], "mrp_range": (60, 90), "shelf_life": (120, 180)},
    {"brand": "Jubilee", "product": "Coconut Biscuit", "category": "Snacks", "variants": ["90g"], "mrp_range": (55, 85), "shelf_life": (120, 180)},
    {"brand": "Sooper", "product": "Cream Biscuit", "category": "Snacks", "variants": ["Half Roll", "Full Roll"], "mrp_range": (35, 90), "shelf_life": (120, 180)},
    {"brand": "Rio", "product": "Wafer", "category": "Snacks", "variants": ["18g", "45g"], "mrp_range": (10, 50), "shelf_life": (120, 180)},
    
    # BAKERY - Fresh baked goods
    {"brand": "English Biscuit", "product": "White Bread", "category": "Bakery", "variants": ["Medium", "Large"], "mrp_range": (80, 140), "shelf_life": (3, 7)},
    {"brand": "English Biscuit", "product": "Brown Bread", "category": "Bakery", "variants": ["Medium", "Large"], "mrp_range": (90, 150), "shelf_life": (3, 7)},
    {"brand": "Peek Freans", "product": "Cake Rusk", "category": "Bakery", "variants": ["Regular", "Chocolate"], "mrp_range": (150, 250), "shelf_life": (60, 90)},
    {"brand": "Young's", "product": "Milk Rusk", "category": "Bakery", "variants": ["300g"], "mrp_range": (140, 180), "shelf_life": (60, 90)},
    {"brand": "Bake Parlor", "product": "Rusk", "category": "Bakery", "variants": ["Plain", "Zeera"], "mrp_range": (120, 200), "shelf_life": (60, 90)},
    {"brand": "Dawn", "product": "White Bread", "category": "Bakery", "variants": ["Medium"], "mrp_range": (85, 120), "shelf_life": (3, 7)},
    
    # GROCERY - Staples, Spices, Cooking
    {"brand": "National", "product": "Biryani Masala", "category": "Grocery", "variants": ["50g", "100g"], "mrp_range": (50, 150), "shelf_life": (365, 730)},
    {"brand": "National", "product": "Korma Masala", "category": "Grocery", "variants": ["50g"], "mrp_range": (50, 100), "shelf_life": (365, 730)},
    {"brand": "National", "product": "Nihari Masala", "category": "Grocery", "variants": ["50g"], "mrp_range": (55, 105), "shelf_life": (365, 730)},
    {"brand": "Shan", "product": "Biryani Masala", "category": "Grocery", "variants": ["50g", "100g"], "mrp_range": (50, 140), "shelf_life": (365, 730)},
    {"brand": "Shan", "product": "Chicken Tikka", "category": "Grocery", "variants": ["50g"], "mrp_range": (60, 95), "shelf_life": (365, 730)},
    {"brand": "Shan", "product": "Chicken Karahi", "category": "Grocery", "variants": ["50g"], "mrp_range": (55, 100), "shelf_life": (365, 730)},
    {"brand": "Mehran", "product": "Seekh Kabab", "category": "Grocery", "variants": ["50g"], "mrp_range": (45, 85), "shelf_life": (365, 730)},
    {"brand": "Dalda", "product": "Cooking Oil", "category": "Grocery", "variants": ["1L", "2.5L", "5L"], "mrp_range": (300, 1500), "shelf_life": (365, 545)},
    {"brand": "Habib", "product": "Banaspati", "category": "Grocery", "variants": ["1kg", "2.5kg"], "mrp_range": (280, 850), "shelf_life": (365, 545)},
    {"brand": "Eva", "product": "Cooking Oil", "category": "Grocery", "variants": ["1L", "5L"], "mrp_range": (290, 1450), "shelf_life": (365, 545)},
    {"brand": "Tapal", "product": "Danedar Tea", "category": "Grocery", "variants": ["190g", "475g", "950g"], "mrp_range": (150, 850), "shelf_life": (365, 730)},
    {"brand": "Lipton", "product": "Yellow Label", "category": "Grocery", "variants": ["200g", "450g"], "mrp_range": (180, 800), "shelf_life": (365, 730)},
    {"brand": "Vital", "product": "Tea", "category": "Grocery", "variants": ["475g"], "mrp_range": (420, 480), "shelf_life": (365, 730)},
    {"brand": "Sufi", "product": "Basmati Rice", "category": "Grocery", "variants": ["5kg", "10kg"], "mrp_range": (800, 1800), "shelf_life": (365, 730)},
    {"brand": "Guard", "product": "Super Basmati", "category": "Grocery", "variants": ["5kg", "10kg"], "mrp_range": (750, 1700), "shelf_life": (365, 730)},
    {"brand": "Mehran", "product": "Basmati Rice", "category": "Grocery", "variants": ["5kg"], "mrp_range": (700, 900), "shelf_life": (365, 730)},
    {"brand": "Shangrila", "product": "Vermicelli", "category": "Grocery", "variants": ["200g", "400g"], "mrp_range": (80, 180), "shelf_life": (180, 365)},
    {"brand": "Kolson", "product": "Macaroni", "category": "Grocery", "variants": ["200g", "400g"], "mrp_range": (70, 160), "shelf_life": (180, 365)},
    {"brand": "National", "product": "Salt", "category": "Grocery", "variants": ["800g"], "mrp_range": (40, 60), "shelf_life": (730, 1095)},
    {"brand": "Shan", "product": "Red Chilli Powder", "category": "Grocery", "variants": ["100g", "200g"], "mrp_range": (100, 250), "shelf_life": (365, 730)},
    {"brand": "National", "product": "Turmeric Powder", "category": "Grocery", "variants": ["100g"], "mrp_range": (80, 120), "shelf_life": (365, 730)},
    
    # PERSONAL CARE - Soaps, Shampoos, Creams
    {"brand": "Lux", "product": "Beauty Soap", "category": "Personal_Care", "variants": ["3-pack", "6-pack"], "mrp_range": (150, 350), "shelf_life": (730, 1095)},
    {"brand": "Lifebuoy", "product": "Soap", "category": "Personal_Care", "variants": ["3-pack", "6-pack"], "mrp_range": (120, 300), "shelf_life": (730, 1095)},
    {"brand": "Safeguard", "product": "Soap", "category": "Personal_Care", "variants": ["3-pack"], "mrp_range": (180, 240), "shelf_life": (730, 1095)},
    {"brand": "Dettol", "product": "Soap", "category": "Personal_Care", "variants": ["3-pack"], "mrp_range": (220, 280), "shelf_life": (730, 1095)},
    {"brand": "Dove", "product": "Beauty Bar", "category": "Personal_Care", "variants": ["100g", "3-pack"], "mrp_range": (120, 380), "shelf_life": (730, 1095)},
    {"brand": "Fair & Lovely", "product": "Cream", "category": "Personal_Care", "variants": ["25g", "50g", "80g"], "mrp_range": (100, 350), "shelf_life": (730, 1095)},
    {"brand": "Pond's", "product": "Face Cream", "category": "Personal_Care", "variants": ["50g"], "mrp_range": (250, 320), "shelf_life": (730, 1095)},
    {"brand": "Pantene", "product": "Shampoo", "category": "Personal_Care", "variants": ["180ml", "400ml", "650ml"], "mrp_range": (250, 650), "shelf_life": (730, 1095)},
    {"brand": "Head & Shoulders", "product": "Shampoo", "category": "Personal_Care", "variants": ["185ml", "400ml"], "mrp_range": (300, 650), "shelf_life": (730, 1095)},
    {"brand": "Sunsilk", "product": "Shampoo", "category": "Personal_Care", "variants": ["180ml", "400ml"], "mrp_range": (220, 520), "shelf_life": (730, 1095)},
    {"brand": "Clear", "product": "Shampoo", "category": "Personal_Care", "variants": ["185ml", "400ml"], "mrp_range": (280, 580), "shelf_life": (730, 1095)},
    {"brand": "Colgate", "product": "Toothpaste", "category": "Personal_Care", "variants": ["75g", "150g"], "mrp_range": (120, 280), "shelf_life": (730, 1095)},
    {"brand": "Close Up", "product": "Toothpaste", "category": "Personal_Care", "variants": ["75g", "150g"], "mrp_range": (110, 260), "shelf_life": (730, 1095)},
    {"brand": "Sensodyne", "product": "Toothpaste", "category": "Personal_Care", "variants": ["75g"], "mrp_range": (280, 350), "shelf_life": (730, 1095)},
    
    # HOUSEHOLD - Detergents, Cleaners
    {"brand": "Surf Excel", "product": "Detergent", "category": "Household", "variants": ["1kg", "3kg", "6kg"], "mrp_range": (250, 1500), "shelf_life": (730, 1095)},
    {"brand": "Ariel", "product": "Detergent", "category": "Household", "variants": ["1kg", "3kg"], "mrp_range": (280, 900), "shelf_life": (730, 1095)},
    {"brand": "Bonus", "product": "Detergent", "category": "Household", "variants": ["1kg", "3kg"], "mrp_range": (220, 750), "shelf_life": (730, 1095)},
    {"brand": "Wheel", "product": "Detergent", "category": "Household", "variants": ["1kg"], "mrp_range": (180, 250), "shelf_life": (730, 1095)},
    {"brand": "Harpic", "product": "Toilet Cleaner", "category": "Household", "variants": ["500ml", "1L"], "mrp_range": (150, 350), "shelf_life": (730, 1095)},
    {"brand": "Dettol", "product": "Disinfectant", "category": "Household", "variants": ["500ml"], "mrp_range": (280, 350), "shelf_life": (730, 1095)},
    {"brand": "Colin", "product": "Glass Cleaner", "category": "Household", "variants": ["500ml"], "mrp_range": (180, 240), "shelf_life": (730, 1095)},
    {"brand": "Vim", "product": "Dishwash Bar", "category": "Household", "variants": ["250g", "500g"], "mrp_range": (60, 140), "shelf_life": (730, 1095)},
    
    # FROZEN - Frozen Foods
    {"brand": "K&N's", "product": "Chicken Nuggets", "category": "Frozen", "variants": ["500g", "1kg"], "mrp_range": (400, 850), "shelf_life": (180, 365)},
    {"brand": "K&N's", "product": "Chicken Seekh Kabab", "category": "Frozen", "variants": ["500g"], "mrp_range": (450, 550), "shelf_life": (180, 365)},
    {"brand": "K&N's", "product": "Chicken Patties", "category": "Frozen", "variants": ["12-pack"], "mrp_range": (380, 450), "shelf_life": (180, 365)},
    {"brand": "Menu", "product": "Plain Paratha", "category": "Frozen", "variants": ["20-pack", "30-pack"], "mrp_range": (250, 480), "shelf_life": (180, 365)},
    {"brand": "Makkhan", "product": "Paratha", "category": "Frozen", "variants": ["20-pack"], "mrp_range": (230, 280), "shelf_life": (180, 365)},
    {"brand": "Walls", "product": "Ice Cream Cornetto", "category": "Frozen", "variants": ["Single"], "mrp_range": (100, 140), "shelf_life": (180, 365)},
    {"brand": "Walls", "product": "Magnum", "category": "Frozen", "variants": ["Single"], "mrp_range": (180, 220), "shelf_life": (180, 365)},
]

# ----------------------------
# 1. CLIENT DATA (SIMPLIFIED)
# ----------------------------
clients = []
for cid in range(1, NUM_CLIENTS + 1):
    clients.append({
        "client_id": f"C{cid:03d}",
        "client_name": f"Store_{cid}",
        "owner_name": f"Owner_{chr(64+cid)}",
        "location": random.choice([
            "Karachi_North", "Karachi_South", "Lahore_DHA", "Lahore_Cantt",
            "Islamabad_F6", "Islamabad_F7", "Rawalpindi_Saddar", 
            "Hyderabad_Latifabad", "Multan_Cantt", "Faisalabad_City"
        ])
    })

clients_df = pd.DataFrame(clients)
clients_df.to_csv("data/clients.csv", index=False)

# ----------------------------
# 2. DISTRIBUTOR PRODUCT MASTER LIST (COMPREHENSIVE REALISTIC)
# ----------------------------
products = []
sku_counter = 1

# Generate products from comprehensive Pakistani brands
for base_product in pakistani_products_base:
    for variant in base_product["variants"]:
        # Generate a sample expiry date for this product type
        # This represents a typical batch's expiry date
        sample_mfg = today - timedelta(days=random.randint(30, 90))
        sample_shelf_life = random.randint(base_product["shelf_life"][0], base_product["shelf_life"][1])
        sample_expiry = sample_mfg + timedelta(days=sample_shelf_life)
        
        products.append({
            "sku": f"SKU{sku_counter:04d}",
            "product_name": f"{base_product['brand']} {base_product['product']} {variant}",
            "brand": base_product['brand'],
            "category": base_product["category"],
            "variant": variant,
            "mrp": round(random.uniform(base_product["mrp_range"][0], base_product["mrp_range"][1]), 2),
            "unit": "pcs",
            "min_shelf_life": base_product["shelf_life"][0],
            "max_shelf_life": base_product["shelf_life"][1],
            "expiry_date": sample_expiry  # Expiry date column added right after shelf life
        })
        sku_counter += 1

products_df = pd.DataFrame(products)
products_df.to_csv("data/distributor_products.csv", index=False)

print(f"✔ Generated {len(products_df)} realistic Pakistani FMCG products")

# ----------------------------
# 3. STOCK BATCH GENERATION (Using realistic shelf life)
# ----------------------------
stock_rows = []
batch_counter = 1

for _, product in products_df.iterrows():
    sku = product["sku"]
    product_name = product["product_name"]
    category = product["category"]

    # Decide how many clients will have this product
    num_clients_with_product = random.randint(3, NUM_CLIENTS)
    selected_clients = random.sample(list(clients_df["client_id"]), num_clients_with_product)

    for client_id in selected_clients:
        for batch_num in range(BATCHES_PER_PRODUCT_PER_CLIENT):
            
            # Manufacturing date in the past
            mfg = today - timedelta(days=random.randint(10, 200))
            
            # Use realistic shelf life from product data
            shelf_life_days = random.randint(product["min_shelf_life"], product["max_shelf_life"])
            
            exp = mfg + timedelta(days=shelf_life_days)
            
            # RSL will be calculated/updated by different code
            rsl_placeholder = (exp - today).days

            stock_rows.append({
                "batch_id": f"BATCH{batch_counter:06d}",
                "client_id": client_id,
                "sku": sku,
                "product_name": product_name,
                "category": category,
                "qty_on_hand": random.randint(10, 300),
                "unit_price": round(product["mrp"] * random.uniform(0.65, 0.95), 2),
                "mfg_date": mfg,
                "exp_date": exp,
                "total_shelf_life_days": shelf_life_days,
                "remaining_shelf_life_days": rsl_placeholder,  # To be updated by formula
                "condition_factor": round(random.uniform(0.9, 1.1), 2)
            })
            
            batch_counter += 1

stock_df = pd.DataFrame(stock_rows)
stock_df.to_csv("data/stock_batches.csv", index=False)

# ----------------------------
# 4. DAILY SALES DATA
# Historical sales for demand forecasting
# Now organized per client with multiple products
# ----------------------------

# Group sales by client
sales_by_client = {}

# Create a mapping of SKU to expiry date from stock_df for realistic expiry dates
sku_expiry_map = {}
for _, stock_item in stock_df.iterrows():
    if stock_item['sku'] not in sku_expiry_map:
        sku_expiry_map[stock_item['sku']] = []
    sku_expiry_map[stock_item['sku']].append(stock_item['exp_date'])

# Iterate through all products
for _, product in products_df.iterrows():
    sku = product["sku"]
    product_name = product["product_name"]
    unit_price = product["mrp"] * random.uniform(0.70, 0.90) # Selling price
    
    # Find which clients have this product
    clients_with_product = stock_df[stock_df["sku"] == sku]["client_id"].unique()
    
    for client_id in clients_with_product:
        # Initialize client's sales list if not exists
        if client_id not in sales_by_client:
            sales_by_client[client_id] = []
        
        # Get expiry dates for this SKU
        expiry_dates = sku_expiry_map.get(sku, [])
        
        # Base daily demand varies per product-client pair
        base_sales = random.randint(2, 25)
        trend_factor = random.choice([0.92, 0.95, 1.0, 1.05, 1.08])
        seasonality = random.uniform(0.8, 1.2)

        for day in range(SALES_DAYS):
            date = today - timedelta(days=SALES_DAYS - day)
            
            # Weekday effect
            weekday_multiplier = [0.7, 0.85, 0.95, 1.1, 1.3, 1.6, 0.8][date.weekday()]
            
            # Calculate quantity sold
            qty_sold = max(0, int(
                base_sales * trend_factor * seasonality * weekday_multiplier 
                + np.random.normal(0, 2.0)
            ))
            
            # Calculate sales amount
            sales_amount = round(qty_sold * unit_price * random.uniform(0.95, 1.05), 2)
            
            # Select a random expiry date from available batches, or generate one
            if expiry_dates:
                exp_date = random.choice(expiry_dates)
            else:
                # Generate expiry date based on product shelf life
                exp_date = date + timedelta(days=random.randint(product["min_shelf_life"], product["max_shelf_life"]))

            sales_by_client[client_id].append({
                "date": date,
                "sku": sku,
                "product_name": product_name,
                "qty_sold": qty_sold,
                "unit_price": round(unit_price, 2),
                "sales_amount": sales_amount,
                "expiry_date": exp_date
            })

# Save separate CSV file for each client
for client_id, sales_data in sales_by_client.items():
    client_df = pd.DataFrame(sales_data)
    # Sort by date then by sku for better readability
    client_df = client_df.sort_values(['date', 'sku']).reset_index(drop=True)
    
    # Save to client-specific file
    filename = f"data/sales_daily_{client_id}.csv"
    client_df.to_csv(filename, index=False)
    
    print(f"✔ Generated {len(client_df)} sales records for {client_id}")

print(f"✔ Created sales files for {len(sales_by_client)} clients")

# ----------------------------
# 5. STOCK NORMS / ALLOCATION TABLE
# Shows how distributor allocates products to clients
# Based on historical sales patterns - will be updated by Prophet/ML model
# ----------------------------
allocation_rows = []

for _, product in products_df.iterrows():
    sku = product["sku"]
    
    # Total distributor inventory for this product
    total_distributor_qty = random.randint(1000, 5000)
    
    # Find which clients have this product in stock
    clients_with_product = stock_df[stock_df["sku"] == sku]["client_id"].unique()
    
    if len(clients_with_product) == 0:
        continue
    
    # Calculate historical average sales per client for this SKU
    client_sales_avg = {}
    for cid in clients_with_product:
        # Read from client-specific sales file
        client_sales_file = f"data/sales_daily_{cid}.csv"
        if os.path.exists(client_sales_file):
            client_sales_df = pd.read_csv(client_sales_file)
            avg_sales = client_sales_df[client_sales_df["sku"] == sku]["qty_sold"].mean()
            client_sales_avg[cid] = max(1, avg_sales if not pd.isna(avg_sales) else 1)
        else:
            client_sales_avg[cid] = 1  # Default if file doesn't exist
    
    total_avg_sales = sum(client_sales_avg.values())
    
    # Allocate based on proportional sales history
    for cid in clients_with_product:
        proportion = client_sales_avg[cid] / total_avg_sales if total_avg_sales > 0 else 1/len(clients_with_product)
        allocated_qty = int(total_distributor_qty * proportion)
        
        # Stock norm = how much should be maintained at client
        # Based on 30 days of average demand + safety stock
        avg_daily_demand = client_sales_avg[cid]
        stock_norm = int(avg_daily_demand * 30 * 1.2)  # 20% safety stock
        
        allocation_rows.append({
            "sku": sku,
            "product_name": product["product_name"],
            "client_id": cid,
            "allocated_qty": allocated_qty,
            "stock_norm": stock_norm,  # To be recalculated by Prophet
            "avg_daily_demand": round(avg_daily_demand, 2),
            "last_updated": today
        })

allocation_df = pd.DataFrame(allocation_rows)
allocation_df.to_csv("data/stock_norms_allocation.csv", index=False)

# ----------------------------
# ENHANCED SUMMARY
# ----------------------------
print("\n" + "="*70)
print("✔ REALISTIC PAKISTANI FMCG DATASET CREATED SUCCESSFULLY!")
print("="*70)
print(f"\n📊 Summary:")
print(f"  • Clients: {len(clients_df)}")
print(f"  • Real Pakistani Products: {len(products_df)}")
print(f"    - Dairy: {len(products_df[products_df['category']=='Dairy'])}")
print(f"    - Beverages: {len(products_df[products_df['category']=='Beverages'])}")
print(f"    - Snacks: {len(products_df[products_df['category']=='Snacks'])}")
print(f"    - Bakery: {len(products_df[products_df['category']=='Bakery'])}")
print(f"    - Grocery: {len(products_df[products_df['category']=='Grocery'])}")
print(f"    - Personal_Care: {len(products_df[products_df['category']=='Personal_Care'])}")
print(f"    - Household: {len(products_df[products_df['category']=='Household'])}")
print(f"    - Frozen: {len(products_df[products_df['category']=='Frozen'])}")
print(f"  • Stock Batches: {len(stock_df)}")
print(f"  • Sales Files: {len(sales_by_client)} client-specific files")
total_sales = sum(len(sales_by_client[cid]) for cid in sales_by_client)
print(f"  • Total Sales Records: {total_sales} (over {SALES_DAYS} days)")
print(f"  • Allocation Records: {len(allocation_df)}")
print(f"\n📁 Files generated in /data:")
print("  1. clients.csv                       - 10 retail stores across Pakistan")
print("  2. distributor_products.csv          - Real Pakistani FMCG brands")
print("  3. stock_batches.csv                 - Batch-level stock with realistic expiry")
print(f"  4. sales_daily_C001.csv to C0{NUM_CLIENTS:02d}.csv - Per-client sales history")
print("  5. stock_norms_allocation.csv        - Distribution allocation norms")
print("\n💡 Realistic Features:")
print("  • Actual Pakistani brands: Nestle, Unilever, P&G, National, Shan, etc.")
print("  • Category-specific shelf life (Dairy: 7-30 days, Frozen: 180-365 days)")
print("  • Realistic MRP based on Pakistani market prices")
print("  • Weighted sales patterns (weekends higher, seasonal trends)")
print("  • Separate sales files per client for better organization")
print("="*70 + "\n")
