from faker import Faker
import pandas as pd
import yaml
import random
from pathlib import Path

# Initialize Faker to generate synthetic data
fake = Faker()

# Set the output location for generated files
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

print("Project 1: ETL Data Generation")
print("Faker initialized successfully!")
print(f"Data files will be saved to: {DATA_DIR}")


# Generate 1,000 synthetic customer records
def generate_customers(num_records=1000):
    customers = []

    # Track emails to ensure they are unique
    used_emails = set()

    for customer_id in range(1, num_records + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Generate a unique email address
        email = fake.unique.email()

        customer = {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": fake.numerify(text="###-###-####"),
            "city": fake.city(),
            "registration_date": fake.date_between(
                start_date="-5y", end_date="today"
            ).isoformat(),
            "customer_type": random.choice(
                ["Regular", "Premium", "VIP"]
            )
        }

        customers.append(customer)

    return pd.DataFrame(customers)


# Test customer generation
customers_df = generate_customers()

print(f"Generated {len(customers_df)} customer records")
print(customers_df.head())


# Generate 1,000 synthetic product records
def generate_products(num_records=1000):
    products = []

    categories = [
        "Electronics", "Clothing", "Home",
        "Beauty", "Sports", "Books"
    ]

    for product_id in range(1, num_records + 1):
        product = {
            "product_id": product_id,
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "brand": fake.company(),
            "unit_price": round(random.uniform(5.00, 500.00), 2),
            "stock_quantity": random.randint(0, 500),
            "is_active": random.choice([True, False])
        }

        products.append(product)

    return pd.DataFrame(products)


# Test product generation
products_df = generate_products()

print(f"\nGenerated {len(products_df)} product records")
print(products_df.head())


# Generate 1,000 synthetic order records
def generate_orders(customers_df, num_records=1000):
    orders = []

    # Use only customer IDs that exist in the customers dataset
    valid_customer_ids = customers_df["customer_id"].tolist()

    for order_id in range(1, num_records + 1):
        total_amount = round(random.uniform(10.00, 1000.00), 2)
        discount_amount = round(
            random.uniform(0, total_amount * 0.20), 2
        )

        order = {
            "order_id": order_id,
            "customer_id": random.choice(valid_customer_ids),
            "order_date": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(sep=" "),
            "order_status": random.choice(
                ["Pending", "Shipped", "Delivered"]
            ),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "PayPal", "Bank Transfer"]
            ),
            "total_amount": total_amount,
            "discount_amount": discount_amount,
            "shipping_cost": round(random.uniform(0, 25.00), 2)
        }

        orders.append(order)

    return pd.DataFrame(orders)


# Test order generation
orders_df = generate_orders(customers_df)

print(f"\nGenerated {len(orders_df)} order records")
print(orders_df.head())

# Validate that all orders reference existing customers
invalid_orders = ~orders_df["customer_id"].isin(
    customers_df["customer_id"]
)

assert not invalid_orders.any(), "Invalid customer IDs found in orders!"
print("Order validation passed: All customer IDs are valid!")


# Save each dataset in CSV, JSON, and YAML formats
def save_data(df, filename):
    try:
        # Save as CSV
        df.to_csv(DATA_DIR / f"{filename}.csv", index=False)

        # Save as JSON
        df.to_json(
            DATA_DIR / f"{filename}.json",
            orient="records",
            indent=4
        )

        # Save as YAML
        with open(
            DATA_DIR / f"{filename}.yaml",
            "w",
            encoding="utf-8"
        ) as file:
            yaml.safe_dump(
                df.to_dict(orient="records"),
                file,
                sort_keys=False
            )

        print(f"Saved {filename} in CSV, JSON, and YAML formats")

    except Exception as e:
        print(f"Error saving {filename}: {e}")
        raise


# Save all three datasets
save_data(customers_df, "customers")
save_data(products_df, "products")
save_data(orders_df, "orders")

print("\nAll 9 data files generated successfully!")


# Validate all 9 saved data files
import json

print("\nValidating saved data files...")

for name in ["customers", "products", "orders"]:
    # Read CSV file
    csv_df = pd.read_csv(DATA_DIR / f"{name}.csv")

    # Read JSON file
    with open(DATA_DIR / f"{name}.json", "r") as file:
        json_data = json.load(file)

    # Read YAML file
    with open(DATA_DIR / f"{name}.yaml", "r") as file:
        yaml_data = yaml.safe_load(file)

    # Check record counts
    assert len(csv_df) == 1000, f"{name}.csv has incorrect count!"
    assert len(json_data) == 1000, f"{name}.json has incorrect count!"
    assert len(yaml_data) == 1000, f"{name}.yaml has incorrect count!"

    print(f"{name}: CSV=1000, JSON=1000, YAML=1000 - PASSED")

print("\nAll 9 files passed validation!")

