
import csv
import json
from utils.classes import Person

def read_csv_file(file_path):
    data = []  # Create an empty list to store the Person objects
    with open(file_path, 'r') as file:  # Open the CSV file in read mode
        csv_reader = csv.reader(file)  # Create a CSV reader object
        header = next(csv_reader)  # Read the header row

        for row in csv_reader:  # Iterate over each row in the CSV file
            person = Person(*row)  # Create a Person object using the row data
            data.append(person)  # Append the Person object to the data list

    return data  # Return the list containing all the Person objects



def read_json_file(file_path):
    with open(file_path, 'r') as file:  # Open the JSON file in read mode
        json_data = json.load(file)  # Parse the JSON data into a Python dictionary
    return json_data  # Return the resulting dictionary
