import csv

file_path1 = 'C:\\Users\\syedr\\OneDrive\\Documents\\Generation-DE\\creme-de-la-creme\\leeds_09-05-2023_09-00-00.csv'
file_path2 = 'C:\\Users\\syedr\\OneDrive\\Documents\\Generation-DE\\creme-de-la-creme\\chesterfield_25-08-2021_09-00-00.csv'

def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        
        print(",".join([header[0], header[1], header[3], "'Item Count'", "'Total Amount'", header[5]]))
        
        data = []
        for row in reader:
            timestamp = row[0]
            location = row[1]
            items_ordered = row[3].split(", ")
            total_amount = float(row[4])  # Convert total_amount to float
            payment_method = row[5]

            item_count = len(items_ordered)

            formatted_row = {
                'timestamp': timestamp,
                'location': location,
                'items_ordered': items_ordered,
                'item_count': item_count,
                'total_amount': total_amount,
                'payment_method': payment_method
            }
            
            data.append(formatted_row)

    return data

if __name__ == '__main__':
    data_list1 = read_csv(file_path1)
    data_list2 = read_csv(file_path2)

    combined_data_list = data_list1 + data_list2

    # Print formatted data for verification
    for row in combined_data_list:
        print(row)

