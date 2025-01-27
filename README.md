# Census Data Processing Project

## Description
This project processes voter demographic data and maps address or ZIP codes to corresponding counties. It utilizes Python to handle data processing and mapping operations for voter information, including address standardization and county lookup functionality.

## Features
- Using the Census Geocoder API
- ZIP code to county mapping
- Address standardization
- Voter data processing
- Asynchronous data handling capabilities

## Requirements
- Python 3.x
- pandas
- asyncio
- aiohttp

## Installation
1. Clone the repository
2. Install required packages


## Data Files Required
The project requires two CSV files:
1. `AMAC_Voters_Data_Religion_wise_bulk.csv` - Contains voter information including:
   - Address
   - City
   - ZIP Code
   - Other demographic data

2. `zip_city_county.csv` - Contains mapping data:
   - ZIP codes
   - Cities
   - Counties

## Usage
1. Ensure your data files are in the project directory
2. Run the main script


## Data Processing
The script performs the following operations:
- Loads and cleans voter data
- Standardizes ZIP codes to 5-digit format
- Using the Census Geocoder API with retries
- Maps ZIP codes (Falied address) and cities to counties





