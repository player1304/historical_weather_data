import csv
import glob
import os
from collections import defaultdict

def get_all_fieldnames(csv_files):
    """Get all unique fieldnames from all CSV files"""
    all_fieldnames = set()
    for file in csv_files:
        with open(file, 'r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            all_fieldnames.update(reader.fieldnames)
    return sorted(list(all_fieldnames))

def merge_csv_files(output_file):
    # Get all CSV files in the current directory
    csv_files = glob.glob('weather_data_*.csv')
    
    if not csv_files:
        print("No CSV files found with the pattern 'weather_data_*.csv'")
        return

    # Get all unique fieldnames
    all_fieldnames = get_all_fieldnames(csv_files)

    # Ensure 'date' and 'city_name' are in the fieldnames
    if 'date' not in all_fieldnames or 'city_name' not in all_fieldnames:
        print("Error: 'date' and 'city_name' columns are required in all CSV files")
        return

    # Write the merged data to the output file
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=all_fieldnames)
        writer.writeheader()

        for file in csv_files:
            print(f"Processing {file}...")
            with open(file, 'r', newline='', encoding='utf-8-sig') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    # Fill missing fields with "NA"
                    for field in all_fieldnames:
                        if field not in row:
                            row[field] = "NA"
                    writer.writerow(row)

    print(f"Merged CSV file created: {output_file}")
    print(f"Total number of columns: {len(all_fieldnames)}")
    print(f"Columns: {', '.join(all_fieldnames)}")

    # Check for duplicates
    check_duplicates(output_file)

def check_duplicates(file_path):
    duplicates = defaultdict(list)
    line_numbers = {}
    
    with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=2):  # start=2 because line 1 is header
            key = (row['date'], row['city_name'])
            duplicates[key].append(i)
            line_numbers[i] = f"{row['date']},{row['city_name']}"

    duplicate_lines = [line for key, lines in duplicates.items() if len(lines) > 1]

    if duplicate_lines:
        print("\nWarning: duplicates found in the following line(s):")
        for line in duplicate_lines:
            print(line_numbers[line])
    else:
        print("\nNo duplicates found.")

if __name__ == "__main__":
    output_file = "weather_data_aggregated.csv"
    merge_csv_files(output_file)