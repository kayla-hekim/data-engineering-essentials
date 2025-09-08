import requests

# URL to fetch
url = "https://s3.amazonaws.com/uvasds-systems/data/SAU-GLOBAL-1-v48-0.csv"

# Fetch the data from the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Save the content to a local file
    with open("SAU-GLOBAL-1-v48-0.csv", "w", encoding="utf-8") as file:
        file.write(response.text)
    print("Data successfully saved to SAU-GLOBAL-1-v48-0.csv")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

# if __name__ == "__main__":
