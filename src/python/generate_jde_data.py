"""
JD Edwards Synthetic Data Generator
====================================
Generates realistic mock data that mimics the quirks of JD Edwards
EnterpriseOne ERP systems, including:
  - Julian dates in CYYDDD format
  - Implicit decimal precision (financial values stored as integers)

Output:
  - data/F0101.csv  (Address Book Master / Customer Dimension)
  - data/F4211.csv  (Sales Order Detail / Transactional Fact)
"""

import os
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Utility: Gregorian → JDE Julian (CYYDDD)
# ---------------------------------------------------------------------------

def date_to_julian(date_obj):
    """
    Converts a Python datetime to JDE CYYDDD format.

    Format breakdown:
      C   = Century indicator (0 = 1900s, 1 = 2000s)
      YY  = Two-digit year
      DDD = Day of year (001–366)

    Example: 2023-01-01 → 123001
    """
    century = 1 if date_obj.year >= 2000 else 0
    yy = date_obj.year % 100
    day_of_year = date_obj.timetuple().tm_yday
    return int(f"{century}{yy:02d}{day_of_year:03d}")


# ---------------------------------------------------------------------------
# F0101 – Address Book Master (Customer Dimension)
# ---------------------------------------------------------------------------

def generate_f0101(num_rows=50):
    """
    Generates the F0101 (Address Book Master) table.
    
    Key columns:
      ABAN8  – Address Number (unique customer ID)
      ABALPH – Alpha Name (company name)
      ABAT1  – Search Type (C = Customer)
      ABAC01 – Category Code (region/segment)
      ABUPMJ – Date Updated (Julian CYYDDD)
    """
    data = []
    for _ in range(num_rows):
        aban8 = fake.unique.random_int(min=10000, max=99999)
        data.append({
            "ABAN8": aban8,
            "ABALPH": fake.company(),
            "ABAT1": "C",  # Customer search type
            "ABAC01": random.choice(["100", "200", "300"]),  # Category Code
            "ABUPMJ": date_to_julian(
                fake.date_between(start_date="-2y", end_date="today")
            ),
        })
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# F4211 – Sales Order Detail (Transactional Fact)
# ---------------------------------------------------------------------------

def generate_f4211(customers_df, num_orders=200):
    """
    Generates the F4211 (Sales Order Detail) table.
    
    Key columns:
      SDDOCO – Document Number (order ID)
      SDDCTO – Order Type (SO = Sales Order)
      SDAN8  – Address Number (FK to F0101)
      SDLITM – Item Number (EAN-13 barcode)
      SDTRDJ – Order Date (Julian CYYDDD)
      SDUORG – Units Ordered (implicit 2-decimal integer)
      SDAEXP – Extended Price (implicit 2-decimal integer)
                e.g. $10.50 stored as 1050
    """
    data = []
    customer_ids = customers_df["ABAN8"].tolist()

    for _ in range(num_orders):
        order_date = fake.date_between(start_date="-1y", end_date="today")
        units = random.randint(1, 100)
        # Price stored as integer with 2 implicit decimals
        # e.g. 1000 = $10.00, 50000 = $500.00
        price_per_unit = random.randint(1000, 50000)
        extended_price = units * price_per_unit

        data.append({
            "SDDOCO": fake.unique.random_int(min=1, max=1000000),
            "SDDCTO": "SO",
            "SDAN8": random.choice(customer_ids),
            "SDLITM": fake.ean13(),
            "SDTRDJ": date_to_julian(order_date),
            "SDUORG": units * 100,  # Implicit 2 decimals for units
            "SDAEXP": extended_price,  # Implicit 2 decimals for price
        })
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

def main():
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("JD Edwards Synthetic Data Generator")
    print("=" * 60)

    # Generate Address Book (Customers)
    print("\n[1/2] Generating F0101 (Address Book Master)...")
    customers = generate_f0101(num_rows=50)
    f0101_path = os.path.join(output_dir, "F0101.csv")
    customers.to_csv(f0101_path, index=False)
    print(f"  ✓ {len(customers)} rows → {f0101_path}")

    # Generate Sales Orders
    print("\n[2/2] Generating F4211 (Sales Order Detail)...")
    orders = generate_f4211(customers, num_orders=200)
    f4211_path = os.path.join(output_dir, "F4211.csv")
    orders.to_csv(f4211_path, index=False)
    print(f"  ✓ {len(orders)} rows → {f4211_path}")

    # Summary
    print("\n" + "=" * 60)
    print("Sample F0101 (first 3 rows):")
    print(customers.head(3).to_string(index=False))
    print(f"\nSample Julian Date: {customers['ABUPMJ'].iloc[0]}")
    print(f"  → Decoded: C={str(customers['ABUPMJ'].iloc[0])[0]}, "
          f"YY={str(customers['ABUPMJ'].iloc[0])[1:3]}, "
          f"DDD={str(customers['ABUPMJ'].iloc[0])[3:6]}")

    print("\nSample F4211 (first 3 rows):")
    print(orders.head(3).to_string(index=False))
    print(f"\nSample SDAEXP (raw): {orders['SDAEXP'].iloc[0]}")
    print(f"  → Actual USD: ${orders['SDAEXP'].iloc[0] / 100:,.2f}")

    print("\n" + "=" * 60)
    print("Done! Upload these CSVs to ADLS Gen2: 01-bronze/landing/")
    print("=" * 60)


if __name__ == "__main__":
    main()
