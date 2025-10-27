from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import numpy as np
import requests
import os
import time
import sqlalchemy
from sqlalchemy import create_engine, text

# ===================================
# DAG Definition
# ===================================

@dag(
    dag_id="airdna_etl_dag",
    description="Full Airdna ETL Pipeline with extraction, transformation, and Azure SQL load",
    schedule_interval="@monthly",
    start_date=datetime(2025, 10, 21),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["airdna", "etl", "azure", "full"],
)
def airdna_etl_pipeline():
    # ------------------------------------------------------------
    # TASK 1 - Extract Airbnb Active Listings
    # ------------------------------------------------------------
    @task()
    def extract1():
        # Load .env variables
        load_success = load_dotenv()
        if load_success:
            logger.info("✅ Environment variables loaded successfully")
        else:
            logger.warning("⚠️ No .env file found. Continuing without environment variables")

        API_KEY = os.getenv("API_KEY")
        API_HOST = "airdna1.p.rapidapi.com"
        url = "https://airdna1.p.rapidapi.com/properties"
        headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": API_HOST}

        # --- List of all locations ---
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


        base_query = {"page_size": "50000", "currency": "usd", "sort": "review_count"}
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

        logger.info(f"✅ Total listings fetched: {len(all_listings)}")
        return all_listings

    # ------------------------------------------------------------
    # TASK 2 - Transform Active Listings
    # ------------------------------------------------------------
    @task()
    def transform1(all_listings):
        airdna_data = pd.json_normalize(all_listings)
        logger.info(f"Initial dataset shape: {airdna_data.shape}")

        # Fill missing property IDs
        try:
            logger.info("=== Starting Airbnb property ID cleanup ===")

            airdna_data["airbnb_property_id"] = (
                airdna_data["airbnb_property_id"]
                .replace(["nan", "None", "", "null"], np.nan)
            )
            airdna_data["airbnb_property_id"] = pd.to_numeric(
                airdna_data["airbnb_property_id"], errors="coerce"
            )
            airdna_data["airbnb_property_id"] = (
                airdna_data["airbnb_property_id"].fillna(0).astype(np.int64)
            )

            # Assign new sequential IDs for nulls
            null_ids = airdna_data["airbnb_property_id"].isnull()
            num_new_ids = null_ids.sum()
            if num_new_ids > 0:
                max_existing_id = airdna_data["airbnb_property_id"].max()
                min_new_id = int(max_existing_id) + 1
                new_ids = np.arange(min_new_id, min_new_id + num_new_ids)
                airdna_data.loc[null_ids, "airbnb_property_id"] = new_ids
                logger.info(f"Assigned {num_new_ids} new IDs.")

            airdna_data["airbnb_property_id"] = (
                airdna_data["airbnb_property_id"].astype(np.int64).abs()
            )

        except Exception as e:
            logger.error(f"Error filling property_id: {e}", exc_info=True)

        # Replace average_daily_rate_ltm
        airdna_data["average_daily_rate"] = airdna_data["average_daily_rate_ltm"].round(2)

        # Drop unnecessary columns
        drop_cols = [
            "location.lat", "location.lng", "vrbo_property_id",
            "market_id", "average_daily_rate_ltm"
        ]
        airdna_data.drop(columns=[c for c in drop_cols if c in airdna_data], inplace=True, errors="ignore")

        # Rename columns
        airdna_data.rename(columns={
            "days_available_ltm": "days_available",
            "occupancy_rate_ltm": "occupancy_rate",
            "revenue_ltm": "revenue",
            "revenue_potential_ltm": "revenue_potential"
        }, inplace=True)

        # Split city/state
        if "city" in airdna_data.columns:
            new_cols = airdna_data["city"].str.split(",", expand=True)
            new_cols.columns = ["City", "State"]
            airdna_data = pd.concat([airdna_data, new_cols], axis=1)
            airdna_data.drop(columns=["city"], inplace=True)

        # Remove duplicates
        airdna_data = airdna_data.loc[:, ~airdna_data.columns.duplicated()]

        logger.info(f"✅ Transform complete. Final shape: {airdna_data.shape}")
        return airdna_data.to_json(orient="records")

    # ------------------------------------------------------------
    # TASK 3 - Extract For-Sale Properties
    # ------------------------------------------------------------
    @task()
    def extract2():
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

        
        url = "https://airdna1.p.rapidapi.com/properties/forsale"
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": "airdna1.p.rapidapi.com",
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
        all_properties = []

        for loc in locations:
            logger.info(f"Fetching for-sale data for {loc}")
            querystring = {
                "location": loc,
                "sort": "estimated_yield",
                "sort_direction": "ascending",
                "currency": "usd",
                "page_size": "50000",
            }

            try:
                response = requests.get(url, headers=headers, params=querystring)
                data = response.json()

                for_sale_data = data.get("for_sale_properties", [])
                for prop in for_sale_data:
                    prop.get("property_details", {}).pop("images", None)
                    all_properties.append(prop)
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error fetching {loc}: {e}")

        logger.info(f"✅ Total for-sale properties fetched: {len(all_properties)}")
        return all_properties

    # ------------------------------------------------------------
    # TASK 4 - Transform For-Sale Properties
    # ------------------------------------------------------------
    @task()
    def transform2(all_properties):
        airdna_data_for_sale = pd.json_normalize(all_properties)
        logger.info(f"Initial shape: {airdna_data_for_sale.shape}")

        drop_cols = [
            "property_details.location.lat", "property_details.location.lng",
            "property_details.market_id", "property_details.market_type",
            "property_details.zoneomics", "property_details.mls_id",
            "property_details.sale_or_rent_type"
        ]
        airdna_data_for_sale.drop(
            columns=[c for c in drop_cols if c in airdna_data_for_sale],
            inplace=True,
            errors="ignore"
        )

        rename_map = {
            "estimates.adr": "Estimated_AverageDailyRate",
            "estimates.estimated_yield": "Estimated_Yield",
            "estimates.occupancy": "Estimated_Occupancy",
            "estimates.revenue": "Estimated_Revenue",
            "estimates.total_comps": "Estimated_TotalComparables",
            "property_details.address": "Property_Address",
            "property_details.bathrooms": "Num_of_Bathrooms",
            "property_details.bedrooms": "Num_of_Bedrooms",
            "property_details.for_sale_property_id": "Property_id",
            "property_details.list_price": "List_Price",
            "property_details.listing_date": "List_Date",
            "property_details.market_name": "City",
            "property_details.square_footage": "Property_SQFT"
        }
        airdna_data_for_sale.rename(columns=rename_map, inplace=True)
        logger.info(f"✅ Transform complete. Final shape: {airdna_data_for_sale.shape}")
        return airdna_data_for_sale.to_json(orient="records")

    # ------------------------------------------------------------
    # TASK 5 - Load Both Datasets to Azure SQL
    # ------------------------------------------------------------
    @task()
    def load(airdna_json, airdna_sale_json):
        airdna_data = pd.read_json(airdna_json)
        airdna_data_for_sale = pd.read_json(airdna_sale_json)

        load_dotenv()
        server = os.getenv("server")
        database = os.getenv("database")
        username = os.getenv("username")
        password = os.getenv("password")

        connection_string = (
            f"mssql+pyodbc://{username}%40{server}:{password}@{server}:1433/{database}"
            "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
        )

        try:
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                logger.info("✅ Connection to Azure SQL successful")
                airdna_data.to_sql("Airbnb_Listings", con=conn, if_exists="append", index=False)
                airdna_data_for_sale.to_sql("Airbnb_ForSale_Properties", con=conn, if_exists="append", index=False)
                logger.info("✅ Data successfully loaded to Azure SQL DB")
        except Exception as e:
            logger.error(f"❌ Database load failed: {e}", exc_info=True)

    # ------------------------------------------------------------
    # DAG TASK DEPENDENCIES
    # ------------------------------------------------------------
    listings = extract1()
    transformed_listings = transform1(listings)

    sale_data = extract2()
    transformed_sale = transform2(sale_data)

    load(transformed_listings, transformed_sale)


# Register the DAG
airdna_etl_dag = airdna_etl_pipeline()
