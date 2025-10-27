# %%
## import all libraries necessary for pipeline

# -----------------------------
# 🧠 Standard library imports
# -----------------------------
import os
import sys
import time
import json
from datetime import datetime, timedelta

# -----------------------------
# 📦 Third-party libraries
# -----------------------------
import requests
import pandas as pd
import numpy as np
import pyodbc
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine

# -----------------------------
# 🚀 Airflow imports
# -----------------------------
##from airflow import DAG

# %%

# %%
# Create logger directories and folders

log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']

for level in log_levels:
    os.makedirs(f"logs/{level.lower()}", exist_ok = True)

# %%
# create each handler for each logger

logger.add(
        "logs/info/info.log",
        level="INFO",
       format= "{time:YYYY-MM-DD HH:mm:ss.SSS} | " \
        "PID:{process.id} | TID{thread.id}| "
        "{message}",
        filter= lambda record: record["level"].name == "INFO"
    )
logger.add(
        "logs/warning/warning.log",
        level="WARNING",
       format= "{time:YYYY-MM-DD HH:mm:ss.SSS} | " \
        "PID:{process.id} | TID{thread.id}| "
        "{message}",
        filter= lambda record: record["level"].name == "WARNING"
    )
logger.add(
        "logs/debug/debug.log",
        level="DEBUG",
       format= "{time:YYYY-MM-DD HH:mm:ss.SSS} | " \
        "PID:{process.id} | TID{thread.id}| "
        "{message}",
        filter= lambda record: record["level"].name == "DEBUG"
    )
logger.add(
        "logs/error/error.log",
        level="ERROR",
       format= "{time:YYYY-MM-DD HH:mm:ss.SSS} | " \
        "PID:{process.id} | TID{thread.id}| "
        "{message}",
        filter= lambda record: record["level"].name == "ERROR"
    )

def extract1():
# %%
## Load credentials API 

    load_success = load_dotenv()
    try:

        if load_success:
            logger.info('Successfully loaded environement variables from .env file')
        else:
            logger.warning('No .env file found or loaded. Continuing without environment variables')
    except ValueError as e:
        print(f"Error: {e}")


    # %%
    ## get credentials for API 

    API_KEY = os.getenv("API_KEY")
    API_HOST = "linkedin-job-search-api.p.rapidapi.com"
    try:
        if API_KEY is None:
            logger.error("API key not found in environment variables. Application may not function correctly.")
            raise ValueError
        else:
            logger.info("environmnet credentials were successfully retrieved.")
    except ValueError as e:
        print(f"Error: {e}")

    # %%
    url = "https://airdna1.p.rapidapi.com/properties"

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "airdna1.p.rapidapi.com"
    }

    locations = [
    "Orlando, Florida",
    "Miami, Florida",
    "Key West, Florida",
    "St. Augustine, Florida",
    "Tampa, Florida",
    "Clearwater, Florida",
    "Kissimmee, Florida",
    "Ft. Walton Beach , Florida"
    "Austin, Texas",
    "San Antonio, Texas",
    "Galveston, Texas",
    "Wilmington, North Carolina",
    "Boone, North Carolina",
    "Lake Norman, North Carolina",
    "Cleveland, Georgia",
    "Helen, Georgia",
    "Columbus, Georgia",
    "Virgina Beach, Virgina",
    "Washington, D.C",
    "Traverse City, Michigan",
    "Ann Arbor, Michigan",
    "Grand Rapids, Michigan",
    # --- Florida ---
    "Jacksonville, Florida",
    "Tallahassee, Florida",
    "Gainesville, Florida",
    "Ocala, Florida",
    "Daytona Beach, Florida",
    "Fort Lauderdale, Florida",
    "West Palm Beach, Florida",
    "Naples, Florida",
    "Fort Myers, Florida",
    "Sarasota, Florida",
    "Pensacola, Florida",
    "Destin, Florida",
    "Panama City Beach, Florida",
    "St. Petersburg, Florida",
    "Port St. Lucie, Florida",

    # --- Michigan ---
    "Detroit, Michigan",
    "Lansing, Michigan",
    "Flint, Michigan",
    "Kalamazoo, Michigan",
    "Battle Creek, Michigan",
    "Muskegon, Michigan",
    "Holland, Michigan",
    "Marquette, Michigan",
    "Petoskey, Michigan",
    "Bay City, Michigan",

    # --- Georgia ---
    "Atlanta, Georgia",
    "Augusta, Georgia",
    "Savannah, Georgia",
    "Macon, Georgia",
    "Albany, Georgia",
    "Valdosta, Georgia",
    "Athens, Georgia",
    "Marietta, Georgia",
    "Roswell, Georgia",
    "Dalton, Georgia",

    # --- North Carolina ---
    "Charlotte, North Carolina",
    "Raleigh, North Carolina",
    "Durham, North Carolina",
    "Greensboro, North Carolina",
    "Asheville, North Carolina",
    "Winston-Salem, North Carolina",
    "Chapel Hill, North Carolina",
    "Cary, North Carolina",
    "Hickory, North Carolina",
    "Fayetteville, North Carolina",

    # --- Texas ---
    "Houston, Texas",
    "Dallas, Texas",
    "Fort Worth, Texas",
    "El Paso, Texas",
    "Lubbock, Texas",
    "Corpus Christi, Texas",
    "Amarillo, Texas",
    "Waco, Texas",
    "Plano, Texas",
    "Denton, Texas",

    # --- Tennessee ---
    "Nashville, Tennessee",
    "Memphis, Tennessee",
    "Knoxville, Tennessee",
    "Chattanooga, Tennessee",
    "Gatlinburg, Tennessee",
    "Pigeon Forge, Tennessee",
    "Franklin, Tennessee",
    "Murfreesboro, Tennessee",
    "Clarksville, Tennessee",
    "Johnson City, Tennessee",

    # --- Illinois ---
    "Chicago, Illinois",
    "Springfield, Illinois",
    "Peoria, Illinois",
    "Rockford, Illinois",
    "Champaign, Illinois",
    "Bloomington, Illinois",
    "Naperville, Illinois",
    "Aurora, Illinois",
    "Decatur, Illinois",
    "Joliet, Illinois",

    # --- Arizona ---
    "Phoenix, Arizona",
    "Tucson, Arizona",
    "Mesa, Arizona",
    "Scottsdale, Arizona",
    "Tempe, Arizona",
    "Flagstaff, Arizona",
    "Sedona, Arizona",
    "Prescott, Arizona",
    "Yuma, Arizona",
    "Glendale, Arizona",

    # --- Utah ---
    "Salt Lake City, Utah",
    "Provo, Utah",
    "Ogden, Utah",
    "Park City, Utah",
    "St. George, Utah",
    "Logan, Utah",
    "Orem, Utah",
    "Moab, Utah",
    "Sandy, Utah",
    "Layton, Utah",

    # --- Oregon ---
    "Portland, Oregon",
    "Eugene, Oregon",
    "Salem, Oregon",
    "Bend, Oregon",
    "Medford, Oregon",
    "Corvallis, Oregon",
    "Ashland, Oregon",
    "Hillsboro, Oregon",
    "Gresham, Oregon",
    "Beaverton, Oregon",

    # --- Montana ---
    "Billings, Montana",
    "Missoula, Montana",
    "Bozeman, Montana",
    "Helena, Montana",
    "Great Falls, Montana",
    "Kalispell, Montana",
    "Butte, Montana",
    "Whitefish, Montana",
    "Livingston, Montana",
    "Big Sky, Montana",

    # --- Nevada ---
    "Las Vegas, Nevada",
    "Reno, Nevada",
    "Carson City, Nevada",
    "Henderson, Nevada",
    "North Las Vegas, Nevada",
    "Elko, Nevada",
    "Mesquite, Nevada",
    "Sparks, Nevada",
    "Pahrump, Nevada",
    "Boulder City, Nevada",

    # --- West Virginia ---
    "Charleston, West Virginia",
    "Morgantown, West Virginia",
    "Huntington, West Virginia",
    "Wheeling, West Virginia",
    "Beckley, West Virginia",
    "Fairmont, West Virginia",
    "Parkersburg, West Virginia",
    "Martinsburg, West Virginia",
    "Bluefield, West Virginia",
    "Clarksburg, West Virginia",

    # --- Wyoming ---
    "Cheyenne, Wyoming",
    "Casper, Wyoming",
    "Jackson, Wyoming",
    "Laramie, Wyoming",
    "Rock Springs, Wyoming",
    "Gillette, Wyoming",
    "Sheridan, Wyoming",
    "Cody, Wyoming",
    "Riverton, Wyoming",
    "Evanston, Wyoming",

    # --- South Dakota ---
    "Sioux Falls, South Dakota",
    "Rapid City, South Dakota",
    "Brookings, South Dakota",
    "Aberdeen, South Dakota",
    "Watertown, South Dakota",
    "Mitchell, South Dakota",
    "Spearfish, South Dakota",
    "Vermillion, South Dakota",
    "Pierre, South Dakota",
    "Huron, South Dakota",

    # --- North Dakota ---
    "Fargo, North Dakota",
    "Bismarck, North Dakota",
    "Grand Forks, North Dakota",
    "Minot, North Dakota",
    "Dickinson, North Dakota",
    "Williston, North Dakota",
    "Wahpeton, North Dakota",
    "Jamestown, North Dakota",
    "West Fargo, North Dakota",
    "Devils Lake, North Dakota",

    # --- South Carolina ---
    "Charleston, South Carolina",
    "Columbia, South Carolina",
    "Greenville, South Carolina",
    "Myrtle Beach, South Carolina",
    "Spartanburg, South Carolina",
    "Florence, South Carolina",
    "Beaufort, South Carolina",
    "Sumter, South Carolina",
    "Rock Hill, South Carolina",
    "Anderson, South Carolina",

    # --- New Mexico ---
    "Albuquerque, New Mexico",
    "Santa Fe, New Mexico",
    "Las Cruces, New Mexico",
    "Roswell, New Mexico",
    "Farmington, New Mexico",
    "Taos, New Mexico",
    "Rio Rancho, New Mexico",
    "Carlsbad, New Mexico",
    "Gallup, New Mexico",
    "Ruidoso, New Mexico",

    # --- Pennsylvania ---
    "Philadelphia, Pennsylvania",
    "Pittsburgh, Pennsylvania",
    "Harrisburg, Pennsylvania",
    "Allentown, Pennsylvania",
    "Erie, Pennsylvania",
    "Scranton, Pennsylvania",
    "Lancaster, Pennsylvania",
    "Bethlehem, Pennsylvania",
    "York, Pennsylvania",
    "State College, Pennsylvania",

    # --- Oklahoma ---
    "Oklahoma City, Oklahoma",
    "Tulsa, Oklahoma",
    "Norman, Oklahoma",
    "Stillwater, Oklahoma",
    "Edmond, Oklahoma",
    "Lawton, Oklahoma",
    "Broken Arrow, Oklahoma",
    "Moore, Oklahoma",
    "Enid, Oklahoma",
    "Ardmore, Oklahoma",

    # --- New Jersey ---
    "Newark, New Jersey",
    "Jersey City, New Jersey",
    "Atlantic City, New Jersey",
    "Trenton, New Jersey",
    "Paterson, New Jersey",
    "Camden, New Jersey",
    "Hoboken, New Jersey",
    "Princeton, New Jersey",
    "Middletown, New Jersey",
    "Cherry Hill, New Jersey",

    # --- Kentucky ---
    "Louisville, Kentucky",
    "Lexington, Kentucky",
    "Bowling Green, Kentucky",
    "Owensboro, Kentucky",
    "Frankfort, Kentucky",
    "Paducah, Kentucky",
    "Covington, Kentucky",
    "Elizabethtown, Kentucky",
    "Richmond, Kentucky",
    "Hopkinsville, Kentucky",

    # --- Kansas ---
    "Wichita, Kansas",
    "Topeka, Kansas",
    "Kansas City, Kansas",
    "Olathe, Kansas",
    "Overland Park, Kansas",
    "Lawrence, Kansas",
    "Manhattan, Kansas",
    "Hutchinson, Kansas",
    "Salina, Kansas",
    "Dodge City, Kansas",

    # --- Louisiana ---
    "New Orleans, Louisiana",
    "Baton Rouge, Louisiana",
    "Shreveport, Louisiana",
    "Lafayette, Louisiana",
    "Lake Charles, Louisiana",
    "Monroe, Louisiana",
    "Alexandria, Louisiana",
    "Hammond, Louisiana",
    "Natchitoches, Louisiana",
    "Slidell, Louisiana",

    # --- Maine ---
    "Portland, Maine",
    "Augusta, Maine",
    "Bangor, Maine",
    "Bar Harbor, Maine",
    "Brunswick, Maine",
    "Lewiston, Maine",
    "Auburn, Maine",
    "Rockland, Maine",
    "Camden, Maine",
    "Kennebunkport, Maine",

    # --- Maryland ---
    "Baltimore, Maryland",
    "Annapolis, Maryland",
    "Frederick, Maryland",
    "Rockville, Maryland",
    "Gaithersburg, Maryland",
    "Silver Spring, Maryland",
    "Ocean City, Maryland",
    "Bethesda, Maryland",
    "Hagerstown, Maryland",
    "College Park, Maryland",

    # --- Massachusetts ---
    "Boston, Massachusetts",
    "Worcester, Massachusetts",
    "Springfield, Massachusetts",
    "Cambridge, Massachusetts",
    "Lowell, Massachusetts",
    "Salem, Massachusetts",
    "New Bedford, Massachusetts",
    "Plymouth, Massachusetts",
    "Gloucester, Massachusetts",
    "Quincy, Massachusetts",

    # --- Colorado ---
    "Denver, Colorado",
    "Colorado Springs, Colorado",
    "Boulder, Colorado",
    "Fort Collins, Colorado",
    "Aspen, Colorado",
    "Vail, Colorado",
    "Breckenridge, Colorado",
    "Durango, Colorado",
    "Grand Junction, Colorado",
    "Pueblo, Colorado",

    # --- Alaska ---
    "Anchorage, Alaska",
    "Juneau, Alaska",
    "Fairbanks, Alaska",
    "Seward, Alaska",
    "Sitka, Alaska",
    "Ketchikan, Alaska",
    "Homer, Alaska",
    "Palmer, Alaska",
    "Wasilla, Alaska",
    "Kenai, Alaska",

    # --- Alabama ---
    "Birmingham, Alabama",
    "Montgomery, Alabama",
    "Mobile, Alabama",
    "Huntsville, Alabama",
    "Tuscaloosa, Alabama",
    "Auburn, Alabama",
    "Florence, Alabama",
    "Gulf Shores, Alabama",
    "Hoover, Alabama",
    "Dothan, Alabama",

    # --- Arkansas ---
    "Little Rock, Arkansas",
    "Fayetteville, Arkansas",
    "Bentonville, Arkansas",
    "Hot Springs, Arkansas",
    "Fort Smith, Arkansas",
    "Jonesboro, Arkansas",
    "Conway, Arkansas",
    "Springdale, Arkansas",
    "Pine Bluff, Arkansas",
    "Rogers, Arkansas",

    # --- New Hampshire ---
    "Manchester, New Hampshire",
    "Nashua, New Hampshire",
    "Concord, New Hampshire",
    "Portsmouth, New Hampshire",
    "Dover, New Hampshire",
    "Keene, New Hampshire",
    "Hanover, New Hampshire",
    "Lebanon, New Hampshire",
    "Exeter, New Hampshire",
    "Claremont, New Hampshire",

    # --- Wisconsin ---
    "Milwaukee, Wisconsin",
    "Madison, Wisconsin",
    "Green Bay, Wisconsin",
    "Kenosha, Wisconsin",
    "Racine, Wisconsin",
    "Appleton, Wisconsin",
    "Eau Claire, Wisconsin",
    "Janesville, Wisconsin",
    "La Crosse, Wisconsin",
    "Oshkosh, Wisconsin"
    ]

    base_query = {
        "page_size": "50000",
        "currency": "usd",
        "sort": "review_count"
    }

    all_listings = []

    for city in locations:
        logger.info(f"🌍 Fetching listings for {city}")
        query = base_query.copy()
        query["location"] = city

        response = requests.get(url, headers=headers, params=query)
        logger.info(f"Response code: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"❌ {city} failed: {response.text}")
            continue

        data = response.json()
        listings = data.get("listings", [])

        if not listings:
            logger.warning(f"⚠️ No data returned for {city}")
            continue

        for listing in listings:
            listing.pop("images", None)
            listing["city"] = city
            all_listings.append(listing)

        logger.info(f"✅ {city}: {len(listings)} listings retrieved")
        time.sleep(1)
def transform1():
    # %%
    # Create a dataframe from get request airbnb data

    airdna_data = pd.json_normalize(all_listings)

    # %%
    # Get the # of rows and columns 

    airdna_data.shape

    # %%
    # View the dataframe created

    airdna_data.head(5)

    # %%
    # Explore what columns have nulls, data type for each column, view all column names

    airdna_data.info()

    # %%
    # Feel in null values for airbnb property IDs


    try:
        logger.info("=== Starting Airbnb property ID cleanup ===")

        # --- Clean up invalid entries ---
        airdna_data['airbnb_property_id'] = (
            airdna_data['airbnb_property_id']
            .replace(["nan", "None", "", "null"], np.nan)
        )

        # Convert to numeric
        airdna_data['airbnb_property_id'] = pd.to_numeric(
            airdna_data['airbnb_property_id'], errors='coerce'
        )

        # Force int64 to prevent overflow
        airdna_data['airbnb_property_id'] = airdna_data['airbnb_property_id'].fillna(0).astype(np.int64)

        # Replace negative or zero IDs with NaN to refill
        airdna_data.loc[airdna_data['airbnb_property_id'] <= 0, 'airbnb_property_id'] = np.nan

        # Identify nulls
        null_ids = airdna_data['airbnb_property_id'].isnull()
        num_new_ids = null_ids.sum()
        logger.info(f"Number of missing or invalid IDs to fill: {num_new_ids}")

        if num_new_ids > 0:
            max_existing_id = airdna_data['airbnb_property_id'].max()
            if pd.isna(max_existing_id) or max_existing_id < 0:
                max_existing_id = 999  # start from 1000 if none exist

            min_new_id = int(max_existing_id) + 1
            new_ids = np.arange(min_new_id, min_new_id + num_new_ids)

            # Assign new sequential positive IDs
            airdna_data.loc[null_ids, 'airbnb_property_id'] = new_ids
            logger.info(
                f"Assigned {num_new_ids} new sequential positive IDs "
                f"from {min_new_id} to {min_new_id + num_new_ids - 1}."
            )

        # Ensure final column is int64 and positive
        airdna_data['airbnb_property_id'] = airdna_data['airbnb_property_id'].astype(np.int64).abs()

        # --- Final checks ---
        remaining_nulls = airdna_data['airbnb_property_id'].isnull().sum()
        remaining_negatives = (airdna_data['airbnb_property_id'] <= 0).sum()

        logger.info(f"Remaining nulls after filling: {remaining_nulls}")
        logger.info(f"Remaining non-positive IDs after filling: {remaining_negatives}")

    except Exception as e:
        logger.error(f"Error filling airbnb_property_id values: {str(e)}", exc_info=True)

    finally:
        logger.info("=== Script finished ===\n")

    # Prevent scientific notation in display
    pd.set_option('display.float_format', '{:.0f}'.format)

    # %%
    # update name of average daily rate
    airdna_data['average_daily_rate'] = airdna_data['average_daily_rate_ltm']

    # Round the average daily rate column to only two decimal places
    airdna_data['average_daily_rate'] = airdna_data['average_daily_rate'].round(2)

    logger.info(f"average_daily_rate new values rounded by 2 {airdna_data['average_daily_rate'].head(5)}")

    # %%
    #Remove columns that are not needed

    airdna_data = airdna_data.drop('location.lat', axis = 1)
    airdna_data = airdna_data.drop('location.lng', axis = 1)
    airdna_data = airdna_data.drop('vrbo_property_id', axis = 1)
    airdna_data = airdna_data.drop('market_id', axis = 1)
    airdna_data = airdna_data.drop('average_daily_rate_ltm', axis = 1)
    logger.info(f"columns left after removing uneeded column {airdna_data.info()}")

    # %%
    #Change column names for columns with ltm at the end

    logger.info(f"airdna table before updating column names{airdna_data.info()}")

    airdna_data = airdna_data.rename(columns={'days_available_ltm': 'days_available', 'occupancy_rate_ltm':'occupancy_rate','revenue_ltm':'revenue','revenue_potential_ltm': 'revenue_potential'})

    logger.info(f"airdna table after updating column names{airdna_data.info()}")

    # %%
    # Remove duplicate columns from table

    logger.info(f"airdna table before removig duplicate columns{airdna_data.info()}")

    airdna_data = airdna_data.loc[:, ~airdna_data.columns.duplicated()]

    logger.info(f"airdna table after removig duplicate columns{airdna_data.info()}")

    # %%
    # split city column to separate city and state and make each a separate column

    try:
        # Log initial column structure
        logger.info(f"Initial columns before removing duplicates: {airdna_data.columns.tolist()}")

        # --- Split 'city' into 'City' and 'State' ---
        if 'city' in airdna_data.columns:
            logger.info("Splitting 'city' column into 'City' and 'State'...")
            new_columns = airdna_data['city'].str.split(',', expand=True)
            new_columns.columns = ['City', 'State']

            # Combine with original DataFrame
            airdna_data = pd.concat([airdna_data, new_columns], axis=1)
            logger.info("Successfully concatenated new columns ['City', 'State'].")

            # Drop old 'city' column
            airdna_data = airdna_data.drop('city', axis=1)
            logger.info("Dropped original 'city' column.")

            # Standardize column casing
            airdna_data.columns = airdna_data.columns.str.title()
            logger.info("Standardized column names to title case.")

        else:
            logger.warning("Column 'city' not found in dataset — skipping split.")

        # --- Log final column structure ---
        logger.info(f"Final columns after processing: {airdna_data.columns.tolist()}")

        # Optional: log a small preview of data after transformation
        logger.debug(f"Data preview after cleanup:\n{airdna_data.head().to_string(index=False)}")

    except Exception as e:
        logger.error(f"Error during 'city' column split and cleanup: {str(e)}", exc_info=True)

    finally:
        logger.info("=== Finished Airdna table cleanup ===\n")


    # %%
    # Remove duplicate rows from table
    logger.info(f"airdna table before removig duplicate columns{airdna_data.columns}")

    airdna_data = airdna_data.loc[:, ~airdna_data.columns.duplicated()]

    logger.info(f"airdna table after removig duplicate columns{airdna_data.columns}")

    # %%
    # review finalized table

    airdna_data.head(5)

def extract2():
    # %%
    locations = [
    "Orlando, Florida",
    "Miami, Florida",
    "Key West, Florida",
    "St. Augustine, Florida",
    "Tampa, Florida",
    "Clearwater, Florida",
    "Kissimmee, Florida",
    "Ft. Walton Beach , Florida"
    "Austin, Texas",
    "San Antonio, Texas",
    "Galveston, Texas",
    "Wilmington, North Carolina",
    "Boone, North Carolina",
    "Lake Norman, North Carolina",
    "Cleveland, Georgia",
    "Helen, Georgia",
    "Columbus, Georgia",
    "Virgina Beach, Virgina",
    "Washington, D.C",
    "Traverse City, Michigan",
    "Ann Arbor, Michigan",
    "Grand Rapids, Michigan",
    # --- Florida ---
    "Jacksonville, Florida",
    "Tallahassee, Florida",
    "Gainesville, Florida",
    "Ocala, Florida",
    "Daytona Beach, Florida",
    "Fort Lauderdale, Florida",
    "West Palm Beach, Florida",
    "Naples, Florida",
    "Fort Myers, Florida",
    "Sarasota, Florida",
    "Pensacola, Florida",
    "Destin, Florida",
    "Panama City Beach, Florida",
    "St. Petersburg, Florida",
    "Port St. Lucie, Florida",

    # --- Michigan ---
    "Detroit, Michigan",
    "Lansing, Michigan",
    "Flint, Michigan",
    "Kalamazoo, Michigan",
    "Battle Creek, Michigan",
    "Muskegon, Michigan",
    "Holland, Michigan",
    "Marquette, Michigan",
    "Petoskey, Michigan",
    "Bay City, Michigan",

    # --- Georgia ---
    "Atlanta, Georgia",
    "Augusta, Georgia",
    "Savannah, Georgia",
    "Macon, Georgia",
    "Albany, Georgia",
    "Valdosta, Georgia",
    "Athens, Georgia",
    "Marietta, Georgia",
    "Roswell, Georgia",
    "Dalton, Georgia",

    # --- North Carolina ---
    "Charlotte, North Carolina",
    "Raleigh, North Carolina",
    "Durham, North Carolina",
    "Greensboro, North Carolina",
    "Asheville, North Carolina",
    "Winston-Salem, North Carolina",
    "Chapel Hill, North Carolina",
    "Cary, North Carolina",
    "Hickory, North Carolina",
    "Fayetteville, North Carolina",

    # --- Texas ---
    "Houston, Texas",
    "Dallas, Texas",
    "Fort Worth, Texas",
    "El Paso, Texas",
    "Lubbock, Texas",
    "Corpus Christi, Texas",
    "Amarillo, Texas",
    "Waco, Texas",
    "Plano, Texas",
    "Denton, Texas",

    # --- Tennessee ---
    "Nashville, Tennessee",
    "Memphis, Tennessee",
    "Knoxville, Tennessee",
    "Chattanooga, Tennessee",
    "Gatlinburg, Tennessee",
    "Pigeon Forge, Tennessee",
    "Franklin, Tennessee",
    "Murfreesboro, Tennessee",
    "Clarksville, Tennessee",
    "Johnson City, Tennessee",

    # --- Illinois ---
    "Chicago, Illinois",
    "Springfield, Illinois",
    "Peoria, Illinois",
    "Rockford, Illinois",
    "Champaign, Illinois",
    "Bloomington, Illinois",
    "Naperville, Illinois",
    "Aurora, Illinois",
    "Decatur, Illinois",
    "Joliet, Illinois",

    # --- Arizona ---
    "Phoenix, Arizona",
    "Tucson, Arizona",
    "Mesa, Arizona",
    "Scottsdale, Arizona",
    "Tempe, Arizona",
    "Flagstaff, Arizona",
    "Sedona, Arizona",
    "Prescott, Arizona",
    "Yuma, Arizona",
    "Glendale, Arizona",

    # --- Utah ---
    "Salt Lake City, Utah",
    "Provo, Utah",
    "Ogden, Utah",
    "Park City, Utah",
    "St. George, Utah",
    "Logan, Utah",
    "Orem, Utah",
    "Moab, Utah",
    "Sandy, Utah",
    "Layton, Utah",

    # --- Oregon ---
    "Portland, Oregon",
    "Eugene, Oregon",
    "Salem, Oregon",
    "Bend, Oregon",
    "Medford, Oregon",
    "Corvallis, Oregon",
    "Ashland, Oregon",
    "Hillsboro, Oregon",
    "Gresham, Oregon",
    "Beaverton, Oregon",

    # --- Montana ---
    "Billings, Montana",
    "Missoula, Montana",
    "Bozeman, Montana",
    "Helena, Montana",
    "Great Falls, Montana",
    "Kalispell, Montana",
    "Butte, Montana",
    "Whitefish, Montana",
    "Livingston, Montana",
    "Big Sky, Montana",

    # --- Nevada ---
    "Las Vegas, Nevada",
    "Reno, Nevada",
    "Carson City, Nevada",
    "Henderson, Nevada",
    "North Las Vegas, Nevada",
    "Elko, Nevada",
    "Mesquite, Nevada",
    "Sparks, Nevada",
    "Pahrump, Nevada",
    "Boulder City, Nevada",

    # --- West Virginia ---
    "Charleston, West Virginia",
    "Morgantown, West Virginia",
    "Huntington, West Virginia",
    "Wheeling, West Virginia",
    "Beckley, West Virginia",
    "Fairmont, West Virginia",
    "Parkersburg, West Virginia",
    "Martinsburg, West Virginia",
    "Bluefield, West Virginia",
    "Clarksburg, West Virginia",

    # --- Wyoming ---
    "Cheyenne, Wyoming",
    "Casper, Wyoming",
    "Jackson, Wyoming",
    "Laramie, Wyoming",
    "Rock Springs, Wyoming",
    "Gillette, Wyoming",
    "Sheridan, Wyoming",
    "Cody, Wyoming",
    "Riverton, Wyoming",
    "Evanston, Wyoming",

    # --- South Dakota ---
    "Sioux Falls, South Dakota",
    "Rapid City, South Dakota",
    "Brookings, South Dakota",
    "Aberdeen, South Dakota",
    "Watertown, South Dakota",
    "Mitchell, South Dakota",
    "Spearfish, South Dakota",
    "Vermillion, South Dakota",
    "Pierre, South Dakota",
    "Huron, South Dakota",

    # --- North Dakota ---
    "Fargo, North Dakota",
    "Bismarck, North Dakota",
    "Grand Forks, North Dakota",
    "Minot, North Dakota",
    "Dickinson, North Dakota",
    "Williston, North Dakota",
    "Wahpeton, North Dakota",
    "Jamestown, North Dakota",
    "West Fargo, North Dakota",
    "Devils Lake, North Dakota",

    # --- South Carolina ---
    "Charleston, South Carolina",
    "Columbia, South Carolina",
    "Greenville, South Carolina",
    "Myrtle Beach, South Carolina",
    "Spartanburg, South Carolina",
    "Florence, South Carolina",
    "Beaufort, South Carolina",
    "Sumter, South Carolina",
    "Rock Hill, South Carolina",
    "Anderson, South Carolina",

    # --- New Mexico ---
    "Albuquerque, New Mexico",
    "Santa Fe, New Mexico",
    "Las Cruces, New Mexico",
    "Roswell, New Mexico",
    "Farmington, New Mexico",
    "Taos, New Mexico",
    "Rio Rancho, New Mexico",
    "Carlsbad, New Mexico",
    "Gallup, New Mexico",
    "Ruidoso, New Mexico",

    # --- Pennsylvania ---
    "Philadelphia, Pennsylvania",
    "Pittsburgh, Pennsylvania",
    "Harrisburg, Pennsylvania",
    "Allentown, Pennsylvania",
    "Erie, Pennsylvania",
    "Scranton, Pennsylvania",
    "Lancaster, Pennsylvania",
    "Bethlehem, Pennsylvania",
    "York, Pennsylvania",
    "State College, Pennsylvania",

    # --- Oklahoma ---
    "Oklahoma City, Oklahoma",
    "Tulsa, Oklahoma",
    "Norman, Oklahoma",
    "Stillwater, Oklahoma",
    "Edmond, Oklahoma",
    "Lawton, Oklahoma",
    "Broken Arrow, Oklahoma",
    "Moore, Oklahoma",
    "Enid, Oklahoma",
    "Ardmore, Oklahoma",

    # --- New Jersey ---
    "Newark, New Jersey",
    "Jersey City, New Jersey",
    "Atlantic City, New Jersey",
    "Trenton, New Jersey",
    "Paterson, New Jersey",
    "Camden, New Jersey",
    "Hoboken, New Jersey",
    "Princeton, New Jersey",
    "Middletown, New Jersey",
    "Cherry Hill, New Jersey",

    # --- Kentucky ---
    "Louisville, Kentucky",
    "Lexington, Kentucky",
    "Bowling Green, Kentucky",
    "Owensboro, Kentucky",
    "Frankfort, Kentucky",
    "Paducah, Kentucky",
    "Covington, Kentucky",
    "Elizabethtown, Kentucky",
    "Richmond, Kentucky",
    "Hopkinsville, Kentucky",

    # --- Kansas ---
    "Wichita, Kansas",
    "Topeka, Kansas",
    "Kansas City, Kansas",
    "Olathe, Kansas",
    "Overland Park, Kansas",
    "Lawrence, Kansas",
    "Manhattan, Kansas",
    "Hutchinson, Kansas",
    "Salina, Kansas",
    "Dodge City, Kansas",

    # --- Louisiana ---
    "New Orleans, Louisiana",
    "Baton Rouge, Louisiana",
    "Shreveport, Louisiana",
    "Lafayette, Louisiana",
    "Lake Charles, Louisiana",
    "Monroe, Louisiana",
    "Alexandria, Louisiana",
    "Hammond, Louisiana",
    "Natchitoches, Louisiana",
    "Slidell, Louisiana",

    # --- Maine ---
    "Portland, Maine",
    "Augusta, Maine",
    "Bangor, Maine",
    "Bar Harbor, Maine",
    "Brunswick, Maine",
    "Lewiston, Maine",
    "Auburn, Maine",
    "Rockland, Maine",
    "Camden, Maine",
    "Kennebunkport, Maine",

    # --- Maryland ---
    "Baltimore, Maryland",
    "Annapolis, Maryland",
    "Frederick, Maryland",
    "Rockville, Maryland",
    "Gaithersburg, Maryland",
    "Silver Spring, Maryland",
    "Ocean City, Maryland",
    "Bethesda, Maryland",
    "Hagerstown, Maryland",
    "College Park, Maryland",

    # --- Massachusetts ---
    "Boston, Massachusetts",
    "Worcester, Massachusetts",
    "Springfield, Massachusetts",
    "Cambridge, Massachusetts",
    "Lowell, Massachusetts",
    "Salem, Massachusetts",
    "New Bedford, Massachusetts",
    "Plymouth, Massachusetts",
    "Gloucester, Massachusetts",
    "Quincy, Massachusetts",

    # --- Colorado ---
    "Denver, Colorado",
    "Colorado Springs, Colorado",
    "Boulder, Colorado",
    "Fort Collins, Colorado",
    "Aspen, Colorado",
    "Vail, Colorado",
    "Breckenridge, Colorado",
    "Durango, Colorado",
    "Grand Junction, Colorado",
    "Pueblo, Colorado",

    # --- Alaska ---
    "Anchorage, Alaska",
    "Juneau, Alaska",
    "Fairbanks, Alaska",
    "Seward, Alaska",
    "Sitka, Alaska",
    "Ketchikan, Alaska",
    "Homer, Alaska",
    "Palmer, Alaska",
    "Wasilla, Alaska",
    "Kenai, Alaska",

    # --- Alabama ---
    "Birmingham, Alabama",
    "Montgomery, Alabama",
    "Mobile, Alabama",
    "Huntsville, Alabama",
    "Tuscaloosa, Alabama",
    "Auburn, Alabama",
    "Florence, Alabama",
    "Gulf Shores, Alabama",
    "Hoover, Alabama",
    "Dothan, Alabama",

    # --- Arkansas ---
    "Little Rock, Arkansas",
    "Fayetteville, Arkansas",
    "Bentonville, Arkansas",
    "Hot Springs, Arkansas",
    "Fort Smith, Arkansas",
    "Jonesboro, Arkansas",
    "Conway, Arkansas",
    "Springdale, Arkansas",
    "Pine Bluff, Arkansas",
    "Rogers, Arkansas",

    # --- New Hampshire ---
    "Manchester, New Hampshire",
    "Nashua, New Hampshire",
    "Concord, New Hampshire",
    "Portsmouth, New Hampshire",
    "Dover, New Hampshire",
    "Keene, New Hampshire",
    "Hanover, New Hampshire",
    "Lebanon, New Hampshire",
    "Exeter, New Hampshire",
    "Claremont, New Hampshire",

    # --- Wisconsin ---
    "Milwaukee, Wisconsin",
    "Madison, Wisconsin",
    "Green Bay, Wisconsin",
    "Kenosha, Wisconsin",
    "Racine, Wisconsin",
    "Appleton, Wisconsin",
    "Eau Claire, Wisconsin",
    "Janesville, Wisconsin",
    "La Crosse, Wisconsin",
    "Oshkosh, Wisconsin"
    ]
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    url = "https://airdna1.p.rapidapi.com/properties/forsale"

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "airdna1.p.rapidapi.com"
    }

    all_properties = []

    for loc in locations:
        print(f"Fetching: {loc}")
        querystring = {
            "location": loc,
            "sort": "estimated_yield",
            "sort_direction": "ascending",
            "currency": "usd",
            "page_size": "50000"
        }

        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=30)
            data = response.json()

            # Each API response has data under "for_sale_properties"
            for_sale_data = data.get("for_sale_properties", [])

            for prop in for_sale_data:
                # Remove the images key if present
                prop.get("property_details", {}).pop("images", None)
                all_properties.append(prop)

            # Optional delay to avoid rate limits
            time.sleep(1)

        except Exception as e:
            print(f"Error fetching {loc}: {e}")
            continue

def transform2():
    # %%
    # Create a dataframe of for sale data

    airdna_data_for_sale = pd.json_normalize(all_properties)

    # %%
    # Get the number of rows, columns

    airdna_data_for_sale.shape

    # %%
    # Explore the data types and # of null values for each column
    airdna_data_for_sale.info()

    # %%
    # View a sample of the table

    airdna_data_for_sale.head(5)

    # %%
    logger.info(f"airdna table before removig duplicate columns{airdna_data_for_sale.info()}")

    columns_to_drop = ['property_details.location.lat','property_details.location.lng','property_details.market_id', 'property_details.market_type','property_details.zoneomics', 'property_details.mls_id','property_details.sale_or_rent_type']
    airdna_data_for_sale.drop(columns = columns_to_drop, inplace =True)

    logger.info(f"airdna table after removig duplicate columns{airdna_data_for_sale.info()}")


    # %%
    # Display the true values of every float column

    pd.reset_option('display.float_format')

    # %%
    # View a sample of the table

    airdna_data_for_sale.head(5)

    # %%
    # update column names of table to be more readable

    logger.info(f"airdna table before removig duplicate columns{airdna_data_for_sale.info()}")

    airdna_data_for_sale = airdna_data_for_sale.rename(columns={
        
    'estimates.adr' : 'Estimated_AverageDailyRate','estimates.estimated_yield' : 'Estimated_Yield',
    'estimates.occupancy' :'Estimated_Occupancy', 'estimates.revenue':'Estimated_Revenue', 
    'estimates.total_comps':'Estimated_TotalComparables', 'property_details.address':'Property_Address', 'property_details.bathrooms':'Num_of_Bathrooms' 
    , 'property_details.bedrooms':'Num of Bedrooms' , 'property_details.for_sale_property_id':'Property_id' , 'property_details.list_price':'List_Price' 
    , 'property_details.listing_date':'List_Date' , 'property_details.market_name':'City' , 'property_details.square_footage':'Property_SQFT' 

    })

    logger.info(f"airdna table after removig duplicate columns{airdna_data_for_sale.info()}")

    # %%
    # View final sample of the table

    airdna_data_for_sale.head(5)

    # %%
    ## Load Azure SQL Database credentials  

    

def load():
    load_success = load_dotenv()
    try:

        if load_success:
            logger.info('Successfully loaded environement variables from .env file')
        else:
            logger.warning('No .env file found or loaded. Continuing without environment variables')
    except ValueError as e:
        print(f"Error: {e}")
    # %%
    ## Get Azure SQL Database credentials  

    try:
        logger.info("Loading environment variables...")

        server = os.getenv("server")
        database = os.getenv("database")
        username = os.getenv("username")
        password = os.getenv("password")

        # Check for missing values
        missing_vars = [
            var_name for var_name, value in [
                ("server", server),
                ("database", database),
                ("username", username),
                ("password", password)
            ] if not value
        ]

        if missing_vars:
            logger.warning(f"Missing environment variables: {', '.join(missing_vars)}")
        else:
            logger.info("All required environment variables loaded successfully.")

        # Optional — log loaded values safely (excluding password)
        logger.info(f"Server: {server}")
        logger.info(f"Database: {database}")
        logger.info(f"Username: {username}")
        logger.info("Password: [HIDDEN]")

    except Exception as e:
        logger.error(f"Error while loading environment variables: {str(e)}", exc_info=True)
    finally:
        logger.info("=== Environment variable loading complete ===\n")


    # %%
    # SQLAlchemy connection string
    connection_string = (
    f"mssql+pyodbc://{username}%40{server}:{password}@{server}:1433/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
)


    try:
        logger.info("Attempting to create SQLAlchemy engine...")
        engine = sqlalchemy.create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            logger.info(f"Successfully connected to Azure SQL Database: {database} on {server}")

    except Exception as e:
        logger.error(f"Failed to connect to Azure SQL Database: {str(e)}", exc_info=True)
    else:
        logger.info("Connection engine created successfully.")
    finally:
        logger.info("=== Database connection setup finished ===\n")

    # %%
    #Load Data into Azure SQL Database
    try:
        logger.info("Starting upload of 'airdna_data' dataframe to Azure SQL table '[Airbnb_Listings]'...")

        airdna_data.to_sql(
            name="[Airbnb_Listings]",       # exact table name in Azure SQL
            con=engine,
            if_exists="append",       # append to existing table
            index=False
        )
        logger.info(f"✅ Successfully uploaded {len(airdna_data)} rows to '[Airbnb_Listings]'.")

    except Exception as e:
        logger.error(f"❌ Failed to upload 'airdna_data': {str(e)}", exc_info=True)

    try:
        logger.info("Starting upload of 'airdna_data_for_sale' dataframe to Azure SQL table '[Airbnb_ForSale_Properties]'...")

        airdna_data_for_sale.to_sql(
            name="[Airbnb_ForSale_Properties]", # exact table name in Azure SQL
            con=engine,
            if_exists="append",
            index=False
        )
        logger.info(f"✅ Successfully uploaded {len(airdna_data_for_sale)} rows to '[Airbnb_ForSale_Properties]'.")

    except Exception as e:
        logger.error(f"❌ Failed to upload 'airdna_data_for_sale': {str(e)}", exc_info=True)

    finally:
        logger.info("=== Data upload process finished ===\n")



