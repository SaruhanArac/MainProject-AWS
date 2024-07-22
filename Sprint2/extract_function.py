import csv
import boto3

def extract_csv_from_s3(bucket_name, filename, download_path):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, filename, download_path)
        print(f"File {filename} downloaded successfully to {download_path}.")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")


def extract_csv_to_list(file):
    data_list = []
    with open(file, mode="r") as csvfile:
        reader = csv.DictReader(csvfile, 
        fieldnames=["Timestamp", "Location", "Customer", "Items", "Total Amount", "Payment Method", "Card Number"])
        for row in reader:
            data_list.append(row)
    return data_list


from datetime import datetime

def transform_data(data_list):
    transformed_data = []
    for row in data_list:
        items = row["Items"].split(", ")
        items_dict = []
        for item in items:
            if " - " in item:
                name, price = item.rsplit(" - ", 1)
                try:
                    items_dict.append({"name": name.strip(), "price": round(float(price.strip()), 2)})
                except ValueError:
                    continue  
            else:
                continue 

        if not items_dict:
            continue 

        try:
            order_total = round(float(row["Total Amount"]), 2)
        except ValueError:
            order_total = 0.0 

        try:
            # Convert the date format from DD/MM/YYYY to YYYY-MM-DD
            transaction_date = datetime.strptime(row["Timestamp"].strip().split()[0], "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            transaction_date = "1970-01-01"  # Default date in case of parsing error

        transaction_time = row["Timestamp"].strip().split()[1]

        transformed_row = {
            "date": transaction_date,
            "time": transaction_time,
            "location": row["Location"].strip(),
            "items": items_dict,  # Storing items as a list of dictionaries
            "order_total": order_total,
            "payment_method": row["Payment Method"].strip(),
            "item_count": len(items_dict)
        }
        transformed_data.append(transformed_row)
    return transformed_data
