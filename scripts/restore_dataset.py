import csv
import os

def restore_csv():
    csv_path = r"D:\dev\csdlpt\dataset\data.csv"
    temp_path = r"D:\dev\csdlpt\dataset\data_restored.csv"
    print(f"Reading and restoring original order of {csv_path}...")
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = []
        for row in reader:
            if row:
                rows.append((int(row[0]), row))
                
    print("Sorting rows by original index...")
    rows.sort(key=lambda x: x[0])
    
    print(f"Writing restored rows to {temp_path}...")
    with open(temp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for _, row in rows:
            writer.writerow(row)
            
    print("Replacing dataset file...")
    os.replace(temp_path, csv_path)
    print("Done!")

if __name__ == "__main__":
    restore_csv()
