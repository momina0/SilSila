import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy import stats
from prophet import Prophet
from hijri_converter import Hijri, Gregorian # type: ignore
import os
import warnings
import logging

# Suppress Prophet/cmdstanpy verbose logging
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')

# ----------------------------
# CONFIGURATION
# ----------------------------
SERVICE_LEVEL = 0.95
Z_SCORE = stats.norm.ppf(SERVICE_LEVEL)
DEFAULT_LEAD_TIME_DAYS = 3
SAFETY_BUFFER_MULTIPLIER = 1.2
MIN_STOCK_NORM = 10
FORECAST_DAYS = 30

# ----------------------------
# DYNAMIC PAKISTANI HOLIDAYS CALCULATOR
# ----------------------------
def get_current_hijri_year():
    """Get current Hijri year dynamically"""
    today_gregorian = Gregorian.today()
    today_hijri = today_gregorian.to_hijri()
    return today_hijri.year

def islamic_to_gregorian_dynamic(hijri_year, hijri_month, hijri_day):
    """Convert Islamic date to Gregorian date dynamically"""
    try:
        hijri_date = Hijri(hijri_year, hijri_month, hijri_day)
        gregorian_date = hijri_date.to_gregorian()
        return f"{gregorian_date.year}-{gregorian_date.month:02d}-{gregorian_date.day:02d}"
    except Exception as e:
        print(f"⚠️  Error converting Hijri date {hijri_year}/{hijri_month}/{hijri_day}: {e}")
        return None

def get_islamic_holidays_dynamic():
    """Generate Islamic holidays dynamically for current and next Hijri years"""
    current_hijri = get_current_hijri_year()
    holidays = []
    
    # Generate for current Hijri year and next 2 years to cover forecast period
    for year_offset in range(0, 3):
        hijri_year = current_hijri + year_offset
        
        # Eid-ul-Fitr (Shawwal 1 - 10th month)
        eid_fitr_date = islamic_to_gregorian_dynamic(hijri_year, 10, 1)
        if eid_fitr_date:
            holidays.append({
                'holiday': 'Eid_ul_Fitr',
                'ds': eid_fitr_date,
                'lower_window': -3,
                'upper_window': 3
            })
        
        # Eid-ul-Adha (Dhu al-Hijjah 10 - 12th month)
        eid_adha_date = islamic_to_gregorian_dynamic(hijri_year, 12, 10)
        if eid_adha_date:
            holidays.append({
                'holiday': 'Eid_ul_Adha',
                'ds': eid_adha_date,
                'lower_window': -3,
                'upper_window': 3
            })
        
        # Ramadan Start (Ramadan 1 - 9th month)
        ramadan_start_date = islamic_to_gregorian_dynamic(hijri_year, 9, 1)
        if ramadan_start_date:
            holidays.append({
                'holiday': 'Ramadan_Start',
                'ds': ramadan_start_date,
                'lower_window': 0,
                'upper_window': 29
            })
        
        # Eid Milad-un-Nabi (Rabi' al-awwal 12 - 3rd month)
        eid_milad_date = islamic_to_gregorian_dynamic(hijri_year, 3, 12)
        if eid_milad_date:
            holidays.append({
                'holiday': 'Eid_Milad',
                'ds': eid_milad_date,
                'lower_window': 0,
                'upper_window': 1
            })
        
        # Ashura (Muharram 10 - 1st month)
        ashura_date = islamic_to_gregorian_dynamic(hijri_year, 1, 10)
        if ashura_date:
            holidays.append({
                'holiday': 'Ashura',
                'ds': ashura_date,
                'lower_window': 0,
                'upper_window': 1
            })
        
        # Shab-e-Barat (Sha'ban 15 - 8th month)
        shab_e_barat_date = islamic_to_gregorian_dynamic(hijri_year, 8, 15)
        if shab_e_barat_date:
            holidays.append({
                'holiday': 'Shab_e_Barat',
                'ds': shab_e_barat_date,
                'lower_window': 0,
                'upper_window': 1
            })
        
        # Shab-e-Qadr (Ramadan 27 - 9th month)
        shab_e_qadr_date = islamic_to_gregorian_dynamic(hijri_year, 9, 27)
        if shab_e_qadr_date:
            holidays.append({
                'holiday': 'Shab_e_Qadr',
                'ds': shab_e_qadr_date,
                'lower_window': 0,
                'upper_window': 1
            })
    
    return holidays

# Get current Hijri and Gregorian years
current_hijri_year = get_current_hijri_year()
current_gregorian_year = datetime.now().year

# Generate dynamic Islamic holidays
all_holidays = get_islamic_holidays_dynamic()

# Add fixed Gregorian holidays for current and next 2 years
for year_offset in range(0, 3):
    year = current_gregorian_year + year_offset
    all_holidays.extend([
        {'holiday': 'Independence_Day', 'ds': f'{year}-08-14', 'lower_window': 0, 'upper_window': 1},
        {'holiday': 'Quaid_e_Azam_Day', 'ds': f'{year}-12-25', 'lower_window': 0, 'upper_window': 1},
        {'holiday': 'Pakistan_Day', 'ds': f'{year}-03-23', 'lower_window': 0, 'upper_window': 1},
        {'holiday': 'Labour_Day', 'ds': f'{year}-05-01', 'lower_window': 0, 'upper_window': 1},
        {'holiday': 'Iqbal_Day', 'ds': f'{year}-11-09', 'lower_window': 0, 'upper_window': 1},
    ])

PAKISTANI_HOLIDAYS = pd.DataFrame(all_holidays)

print("="*70)
print("STOCK NORM CALCULATION SYSTEM WITH PROPHET FORECASTING")
print("="*70)
print(f"\nConfiguration:")
print(f"  • Service Level: {SERVICE_LEVEL*100}%")
print(f"  • Z-Score: {Z_SCORE:.2f}")
print(f"  • Default Lead Time: {DEFAULT_LEAD_TIME_DAYS} days")
print(f"  • Safety Buffer Multiplier: {SAFETY_BUFFER_MULTIPLIER}x")
print(f"  • Forecast Horizon: {FORECAST_DAYS} days")
print(f"  • Using Prophet with DYNAMIC Islamic calendar")
print(f"  • Current Hijri Year: {current_hijri_year} AH")
print(f"  • Current Gregorian Year: {current_gregorian_year} CE")
print(f"\n📅 Dynamic Islamic Holidays Calculated:")
islamic_holidays_only = [h for h in all_holidays if h['holiday'] in ['Eid_ul_Fitr', 'Eid_ul_Adha', 'Ramadan_Start', 'Eid_Milad', 'Ashura', 'Shab_e_Barat', 'Shab_e_Qadr']]
for holiday in islamic_holidays_only[:7]:  # Show first 7 (current year)
    print(f"  • {holiday['holiday']}: {holiday['ds']}")
print(f"\n  Total Holidays Loaded: {len(all_holidays)}")
print("="*70 + "\n")

# ----------------------------
# LOAD DATA
# ----------------------------
print("📂 Loading data...")

# Load distributor products
products_df = pd.read_csv("data/distributor_products.csv")
print(f"✔ Loaded {len(products_df)} products")

# Load clients
clients_df = pd.read_csv("data/clients.csv")
print(f"✔ Loaded {len(clients_df)} clients")

# Get list of all client sales files
client_sales_files = [f for f in os.listdir("data") if f.startswith("sales_daily_C") and f.endswith(".csv")]
print(f"✔ Found {len(client_sales_files)} client sales files\n")

# ----------------------------
# PROPHET DEMAND FORECASTING FUNCTION
# ----------------------------
def forecast_demand_with_prophet(sales_df, sku, forecast_days=FORECAST_DAYS):
    """Use Prophet to forecast future demand for a SKU"""
    sku_sales = sales_df[sales_df['sku'] == sku].copy()
    
    if len(sku_sales) < 14:
        return None
    
    sku_sales['ds'] = pd.to_datetime(sku_sales['date'])
    sku_sales['y'] = sku_sales['qty_sold']
    
    prophet_df = sku_sales.groupby('ds')['y'].sum().reset_index()
    
    date_range = pd.date_range(start=prophet_df['ds'].min(), end=prophet_df['ds'].max(), freq='D')
    prophet_df = prophet_df.set_index('ds').reindex(date_range, fill_value=0).reset_index()
    prophet_df.columns = ['ds', 'y']
    
    try:
        model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=True,
            holidays=PAKISTANI_HOLIDAYS,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05,
            interval_width=0.95
        )
        
        model.stan_backend.logger = logging.getLogger('prophet')
        model.stan_backend.logger.setLevel(logging.ERROR)
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(prophet_df)
        
        future = model.make_future_dataframe(periods=forecast_days)
        forecast = model.predict(future)
        future_forecast = forecast[forecast['ds'] > prophet_df['ds'].max()]
        
        forecasted_avg_demand = future_forecast['yhat'].mean()
        forecasted_std_demand = future_forecast['yhat'].std()
        
        forecasted_avg_demand = max(0, forecasted_avg_demand)
        forecasted_std_demand = max(0, forecasted_std_demand)
        
        if forecasted_std_demand == 0 or pd.isna(forecasted_std_demand):
            forecasted_std_demand = forecasted_avg_demand * 0.3
        
        if forecasted_avg_demand <= 0:
            forecasted_avg_demand = prophet_df['y'].mean()
        
        return {
            'avg_demand': forecasted_avg_demand,
            'std_demand': forecasted_std_demand,
            'forecast_df': future_forecast,
            'historical_avg': prophet_df['y'].mean(),
            'forecast_method': 'Prophet'
        }
        
    except Exception as e:
        print(f"    ⚠️  Prophet failed, using fallback: {str(e)[:50]}")
        return {
            'avg_demand': prophet_df['y'].mean(),
            'std_demand': prophet_df['y'].std(),
            'forecast_df': None,
            'historical_avg': prophet_df['y'].mean(),
            'forecast_method': 'Fallback'
        }

# ----------------------------
# STOCK NORM CALCULATION FUNCTION
# ----------------------------
def calculate_stock_norm(sales_df, sku, product_info, lead_time=DEFAULT_LEAD_TIME_DAYS):
    """Calculate stock norm for a specific SKU using Prophet forecasting"""
    forecast_result = forecast_demand_with_prophet(sales_df, sku)
    
    if forecast_result is None:
        return None
    
    avg_demand = forecast_result['avg_demand']
    std_demand = forecast_result['std_demand']
    forecast_method = forecast_result['forecast_method']
    
    safety_stock = Z_SCORE * std_demand * np.sqrt(lead_time)
    lead_time_demand = avg_demand * lead_time
    rop = lead_time_demand + safety_stock
    stock_norm = rop * SAFETY_BUFFER_MULTIPLIER
    
    if product_info is not None:
        max_shelf_life = product_info.get('max_shelf_life', 365)
        
        if max_shelf_life < 90:
            max_feasible_stock = avg_demand * min(max_shelf_life * 0.7, 60)
            stock_norm = min(stock_norm, max_feasible_stock)
        
        if max_shelf_life < 30:
            max_feasible_stock = avg_demand * min(max_shelf_life * 0.5, 20)
            stock_norm = min(stock_norm, max_feasible_stock)
    
    stock_norm = max(stock_norm, MIN_STOCK_NORM)
    
    order_cycle_days = 7
    optimal_order_qty = avg_demand * order_cycle_days
    
    return {
        'avg_daily_demand': round(avg_demand, 2),
        'std_demand': round(std_demand, 2),
        'coefficient_of_variation': round(std_demand / avg_demand if avg_demand > 0 else 0, 2),
        'lead_time_days': lead_time,
        'safety_stock': round(safety_stock, 2),
        'lead_time_demand': round(lead_time_demand, 2),
        'reorder_point': round(rop, 2),
        'stock_norm': round(stock_norm, 2),
        'optimal_order_qty': round(optimal_order_qty, 2),
        'forecast_method': forecast_method,
        'historical_avg': round(forecast_result['historical_avg'], 2)
    }

# ----------------------------
# PROCESS ALL CLIENTS
# ----------------------------
all_norms = []
today = datetime.now().date()

print("🔄 Processing clients...\n")

for sales_file in client_sales_files:
    client_id = sales_file.replace("sales_daily_", "").replace(".csv", "")
    
    print(f"Processing {client_id}...", end=' ')
    
    try:
        sales_df = pd.read_csv(f"data/{sales_file}")
    except Exception as e:
        print(f"  ⚠️  Error loading {sales_file}: {e}")
        continue
    
    if len(sales_df) == 0:
        print(f"  ⚠️  No sales data")
        continue
    
    unique_skus = sales_df['sku'].unique()
    processed_count = 0
    total_skus = len(unique_skus)
    
    for idx, sku in enumerate(unique_skus, 1):
        if idx % 10 == 0:
            print(f"{idx}/{total_skus}", end=' ', flush=True)
        
        product_info = products_df[products_df['sku'] == sku]
        
        if len(product_info) == 0:
            continue
        
        product_info = product_info.iloc[0].to_dict()
        
        norm_data = calculate_stock_norm(sales_df, sku, product_info)
        
        if norm_data is None:
            continue
        
        norm_record = {
            'client_id': client_id,
            'sku': sku,
            'product_name': product_info['product_name'],
            'brand': product_info['brand'],
            'category': product_info['category'],
            'avg_daily_demand': norm_data['avg_daily_demand'],
            'std_demand': norm_data['std_demand'],
            'coefficient_of_variation': norm_data['coefficient_of_variation'],
            'lead_time_days': norm_data['lead_time_days'],
            'safety_stock': norm_data['safety_stock'],
            'lead_time_demand': norm_data['lead_time_demand'],
            'reorder_point': norm_data['reorder_point'],
            'stock_norm': norm_data['stock_norm'],
            'optimal_order_qty': norm_data['optimal_order_qty'],
            'forecast_method': norm_data['forecast_method'],
            'service_level': SERVICE_LEVEL,
            'z_score': round(Z_SCORE, 2),
            'last_updated': today
        }
        
        all_norms.append(norm_record)
        processed_count += 1
    
    print(f"✔ Processed {processed_count} SKUs")

print(f"\n✔ Total norms calculated: {len(all_norms)}\n")

# ----------------------------
# SAVE RESULTS
# ----------------------------
if len(all_norms) > 0:
    norms_df = pd.DataFrame(all_norms)
    norms_df = norms_df.sort_values(['client_id', 'category', 'product_name'])
    
    output_file = "data/stock_norms_calculated.csv"
    norms_df.to_csv(output_file, index=False)
    
    print(f"💾 Saved stock norms to: {output_file}")
    
    # ----------------------------
    # GENERATE SUMMARY STATISTICS
    # ----------------------------
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    
    print(f"\n📊 Overall Statistics:")
    print(f"  • Total SKU-Client Combinations: {len(norms_df)}")
    print(f"  • Unique Clients: {norms_df['client_id'].nunique()}")
    print(f"  • Unique SKUs: {norms_df['sku'].nunique()}")
    print(f"  • Prophet Forecasts: {len(norms_df[norms_df['forecast_method']=='Prophet'])}")
    print(f"  • Fallback Method: {len(norms_df[norms_df['forecast_method']=='Fallback'])}")
    
    print(f"\n📈 Demand Statistics:")
    print(f"  • Average Daily Demand (mean): {norms_df['avg_daily_demand'].mean():.2f}")
    print(f"  • Average Daily Demand (median): {norms_df['avg_daily_demand'].median():.2f}")
    print(f"  • Average CV: {norms_df['coefficient_of_variation'].mean():.2f}")
    
    print(f"\n📦 Stock Norm Statistics:")
    print(f"  • Average Stock Norm: {norms_df['stock_norm'].mean():.2f}")
    print(f"  • Median Stock Norm: {norms_df['stock_norm'].median():.2f}")
    print(f"  • Min Stock Norm: {norms_df['stock_norm'].min():.2f}")
    print(f"  • Max Stock Norm: {norms_df['stock_norm'].max():.2f}")
    
    print(f"\n🎯 Reorder Point Statistics:")
    print(f"  • Average ROP: {norms_df['reorder_point'].mean():.2f}")
    print(f"  • Average Safety Stock: {norms_df['safety_stock'].mean():.2f}")
    print(f"  • Average Optimal Order Qty: {norms_df['optimal_order_qty'].mean():.2f}")
    
    print(f"\n📋 Category Breakdown:")
    category_summary = norms_df.groupby('category').agg({
        'stock_norm': ['mean', 'count'],
        'avg_daily_demand': 'mean',
        'optimal_order_qty': 'mean'
    }).round(2)
    print(category_summary)
    
    # ----------------------------
    # IDENTIFY KEY INSIGHTS
    # ----------------------------
    print("\n" + "="*70)
    print("KEY INSIGHTS")
    print("="*70)
    
    high_variability = norms_df[norms_df['coefficient_of_variation'] > 1.0]
    print(f"\n⚠️  High Variability SKUs (CV > 1.0): {len(high_variability)}")
    if len(high_variability) > 0:
        print("   Top 5 most variable:")
        print(high_variability.nlargest(5, 'coefficient_of_variation')[
            ['client_id', 'product_name', 'coefficient_of_variation', 'stock_norm']
        ].to_string(index=False))
    
    high_demand = norms_df[norms_df['avg_daily_demand'] > norms_df['avg_daily_demand'].quantile(0.9)]
    print(f"\n📈 High Demand SKUs (top 10%): {len(high_demand)}")
    if len(high_demand) > 0:
        print("   Top 5 highest demand:")
        print(high_demand.nlargest(5, 'avg_daily_demand')[
            ['client_id', 'product_name', 'avg_daily_demand', 'stock_norm']
        ].to_string(index=False))
    
    perishable = norms_df[norms_df['category'].isin(['Dairy', 'Bakery'])]
    print(f"\n🧊 Perishable Items (Dairy/Bakery): {len(perishable)}")
    print(f"   Average Stock Norm: {perishable['stock_norm'].mean():.2f}")
    print(f"   Average Daily Demand: {perishable['avg_daily_demand'].mean():.2f}")
    
    print("\n" + "="*70)
    print("✅ STOCK NORM CALCULATION COMPLETED SUCCESSFULLY!")
    print("="*70)
    print(f"\n💡 Next Steps:")
    print(f"  1. Review calculated stock norms in: {output_file}")
    print("  2. Stock norms include Prophet-based demand forecasting")
    print("  3. Considers Pakistani holidays and seasonality")
    print("  4. Use for automated reorder point alerts")
    print("  5. Integrate with redistribution logic")
    print("  6. Re-run monthly to update with latest sales data")
    print("="*70 + "\n")
    
else:
    print("❌ No stock norms calculated. Check your sales data.")
