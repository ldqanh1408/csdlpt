import csv
import os

def sort_csv():
    csv_path = r"D:\dev\csdlpt\dataset\data.csv"
    temp_path = r"D:\dev\csdlpt\dataset\data_sorted.csv"
    print(f"Reading and sorting {csv_path}...")
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        # Store as tuples to reduce memory overhead
        rows = []
        for row in reader:
            if row:
                rows.append((float(row[2]), row))
                
    print("Sorting rows...")
    rows.sort(key=lambda x: x[0])
    
    print(f"Writing sorted rows to {temp_path}...")
    with open(temp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for _, row in rows:
            writer.writerow(row)
            
    print("Replacing original file...")
    os.replace(temp_path, csv_path)
    print("Done!")

if __name__ == "__main__":
    sort_csv()
