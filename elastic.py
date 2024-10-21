from elasticsearch import Elasticsearch
import csv

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200/")

# Function to create a collection (index)
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Index {p_collection_name} created.")
    else:
        print(f"Index {p_collection_name} already exists.")

# Function to index data into a collection, excluding a specified column
def indexData(p_collection_name, p_exclude_column):
    # Open the employee data CSV file
    with open('S://Employee Sample Data 1.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
       
        # Index each row into Elasticsearch, excluding the specified column
        for row in reader:
            if p_exclude_column in row:
                del row[p_exclude_column]  # Exclude the column
            es.index(index=p_collection_name, document=row)
        print(f"Data indexed into {p_collection_name}, excluding column {p_exclude_column}.")

# Function to search within a collection by column name and value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    print(f"Running query: {query}")
    result = es.search(index=p_collection_name, body=query)
    print(f"Search results for {p_column_name} = {p_column_value}:")
    for hit in result['hits']['hits']:
        print(hit['_source'])
    if not result['hits']['hits']:
        print("No results found.")

# Function to get employee count from a collection
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)['count']
    print(f"Total employees in {p_collection_name}: {count}")
    return count


# Function to delete an employee by ID from a collection
def delEmpById(p_collection_name, p_employee_id):
    try:
        # Attempt to retrieve the document to check if it exists
        result = es.get(index=p_collection_name, id=p_employee_id)
       
        # If the document exists, proceed to delete it
        if result['_id'] == p_employee_id:  # Ensure the ID matches
            es.delete(index=p_collection_name, id=p_employee_id)
            print(f"Deleted employee {p_employee_id} from {p_collection_name}.")
    except elasticsearch.NotFoundError:
        print(f"Employee ID {p_employee_id} not found in {p_collection_name}.")
    except Exception as e:
        print(f"Error occurred while trying to delete employee ID {p_employee_id}: {str(e)}")


# Function to get department facets (count of employees grouped by department)
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    print("Department facet results:")
    for bucket in result['aggregations']['department_count']['buckets']:
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")

# Function to view all documents in a collection
def viewAllDocuments(p_collection_name):
    result = es.search(index=p_collection_name, body={"query": {"match_all": {}}})
    print(f"All documents in {p_collection_name}:")
    for hit in result['hits']['hits']:
        print(hit['_source'])

def deleteCollection(p_collection_name):
    if es.indices.exists(index=p_collection_name):
        es.indices.delete(index=p_collection_name)
        print(f"Index {p_collection_name} deleted.")
    else:
        print(f"Index {p_collection_name} does not exist.")
       

# ----------- Execution Steps -------------
# Define the collection names
v_nameCollection = 'hash_subha'  # Replace 'YourName' with your actual name
v_phoneCollection = 'hash_2861'           # Replace '1234' with the last four digits of your phone number

'''deleteCollection(v_nameCollection)  # Delete the name collection
deleteCollection(v_phoneCollection)'''

# Step 1: Create the collections
#createCollection(v_nameCollection)
#createCollection(v_phoneCollection)

# Step 2: Get employee count before indexing
#getEmpCount(v_nameCollection)

# Step 3: Index data into the collections, excluding specified columns
#indexData(v_nameCollection, 'Department')   # Exclude 'Department' column
#indexData(v_phoneCollection, 'Gender')      # Exclude 'Gender' column

# Step 4: View all documents in the collections to find the correct IDs
#viewAllDocuments(v_nameCollection)

# Step 5: Delete an employee by ID
#delEmpById(v_nameCollection, 'E02003')      # Attempt to delete employee with ID 'E02003'

# Step 6: Get employee count after deletion
#getEmpCount(v_nameCollection)

# Step 7: Search for employees by column values
#searchByColumn(v_nameCollection, 'Department', 'IT')     # Search by 'Department' in v_nameCollection
#searchByColumn(v_nameCollection, 'Gender', 'Male')       # Search by 'Gender' in v_nameCollection
#searchByColumn(v_phoneCollection, 'Department', 'IT')    # Search by 'Department' in v_phoneCollection

# Step 8: Get department facets (grouped counts)
#getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
