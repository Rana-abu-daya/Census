import pandas as pd
import asyncio
import aiohttp
import time
## Final Version
pd.set_option('display.max_columns', None)
# Load voter data
voter_data = pd.read_csv("AMAC_Voters_Data_Religion_wise_bulk.csv")
voter_data['Address'] = voter_data['Address'].str.strip()
voter_data['City'] = voter_data['City'].str.strip()
voter_data['Zip Code'] = voter_data['Zip Code'].astype(str).str.zfill(5)

# voter_data = voter_data[1:1000].copy()
# Load the ZIP-city-to-county mapping
zip_city_county_mapping = pd.read_csv("zip_city_county.csv")
zip_city_county_mapping['zip'] = zip_city_county_mapping['zip'].astype(str).str.zfill(5)


# Fallback function to get county from the ZIP-city-to-county mapping
def get_county_from_mapping(zip_code, city):
    # Ensure ZIP code is treated as a string
    zip_code = str(zip_code).zfill(5)
    # Filter the mapping for matching ZIP code and city
    match1 = zip_city_county_mapping[
        (zip_city_county_mapping['zip'] == zip_code) &
        (zip_city_county_mapping['city'].str.lower() == city.lower())
        ]
    if not match1.empty:
        print(f"\t--> Match found for {city}, {zip_code}")
        return match1.iloc[0]['county_name']
    # Filter the mapping for matching ZIP code and city
    match2 = zip_city_county_mapping[zip_city_county_mapping['zip'] == zip_code]
    if not match2.empty:
        print(f"\t--> Match found for {zip_code}")
        return match2.iloc[0]['county_name']
    return None


# Function to get county using the Census Geocoder API with retries
async def get_county(session, address, city, zip_code, retries=3, delay=2):
    base_url = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
    zip_code = zip_code if len(zip_code) == 5 else zip_code[:5]
    address_var = f"{address}, {city}, {zip_code}"
    params = {
        "address": address_var,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json"
    }

    for attempt in range(retries):
        try:
            async with session.get(base_url, params=params, ssl=False) as response:
                # Check for a valid content type
                content_type = response.headers.get("Content-Type", "")
                if response.status == 200 and "application/json" in content_type:
                    result = await response.json()
                    if result.get('result') and result['result'].get('addressMatches'):
                        county_info = result['result']['addressMatches'][0]['geographies']['Counties'][0]['NAME']
                        return county_info , "API"
                    else:
                        print(f"No API match found for {address}, {city}, {zip_code}")
                        return get_county_from_mapping(zip_code, city), "File Mapping"
                else:
                    print(f"Invalid response for {address}, {city}, {zip_code}: {await response.text()}")
        except Exception as e:
            print(f"Error for {address}, {city}, {zip_code} on attempt {attempt + 1}: {e}")

        # Wait before retrying
        print(f"Retrying {address}, {city}, {zip_code} (Attempt {attempt + 1}/{retries})...")
        await asyncio.sleep(delay)

    # If all retries fail, return fallback
    print(f"All attempts failed for {address}, {city}, {zip_code}. Using fallback.")
    return get_county_from_mapping(zip_code, city), "File Mapping"


# Function to process data asynchronously
async def process_data(voter_data):
    failed_requests = []  # To log failed requests
    counties = []  # To store counties
    sources = []  # To store sources (API or File Mapping)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in voter_data.iterrows():
            task = get_county(session, row['Address'], row['City'], row['Zip Code'])
            tasks.append(task)

        # Execute tasks and collect results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle results and log failed requests
        for i, result in enumerate(results):
            if isinstance(result, Exception) or result is None:
                failed_requests.append({
                    "Address": voter_data.iloc[i]['Address'],
                    "City": voter_data.iloc[i]['City'],
                    "Zip Code": voter_data.iloc[i]['Zip Code']
                })
                counties.append(None)
                sources.append(None)
            else:
                county_info, source = result
                counties.append(county_info)
                sources.append(source)

        # Save failed requests for further debugging
        if failed_requests:
            pd.DataFrame(failed_requests).to_csv("failed_requests.csv", index=False)
            print(f"{len(failed_requests)} requests failed and were logged to 'failed_requests.csv'.")

        return counties, sources

# Run the async process
async def main():
    counties, sources = await process_data(voter_data)
    voter_data['County'] = counties
    voter_data['Source'] = sources


asyncio.run(main())

# Save the results
output_path = "voter_data_with_counties.csv"
voter_data.to_csv(output_path, index=False)

print(f"County mapping completed and saved to {output_path}")
print(f"{len(voter_data['Voters Id']) - len(voter_data['County'])} failed requests were NOT resolved.")

# Load the updated voter data with county names
voter_data_with_counties = pd.read_csv("voter_data_with_counties.csv")

# Group by county name and count the rows for each county
county_counts = voter_data_with_counties.groupby('County').size().reset_index(name='Count')

# Save the result to a new CSV file
output_path_counts = "county_counts.csv"
county_counts.to_csv(output_path_counts, index=False)

print(f"County counts saved to {output_path_counts}")

