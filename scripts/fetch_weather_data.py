import requests
import json
import logging
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

def fetch_live_weather(lat: float, lon: float) -> Optional[Dict]:
    """
    Fetches live weather data from Open-Meteo for a specific coordinate.
    Requires no API key.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m"],
        "wind_speed_unit": "mph", # standard US unit for fire spread context
        "temperature_unit": "fahrenheit", # easier for US context
        "timezone": "America/Los_Angeles"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        current = data.get("current", {})
        
        # Ensure we got all the required fields
        if not all(k in current for k in ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m"]):
            logger.error(f"Incomplete weather payload for {lat},{lon}")
            return None

        # Return a clean dictionary for our Kafka payload
        weather_payload = {
            "temperature_f": current["temperature_2m"],
            "humidity_percent": current["relative_humidity_2m"],
            "wind_speed_mph": current["wind_speed_10m"],
            "wind_direction_deg": current["wind_direction_10m"],
            "timestamp": current["time"]
        }
        
        return weather_payload

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch weather data for {lat},{lon}: {e}")
        return None

if __name__ == "__main__":
    # Test with Riverside coordinates (33.9533, -117.3961)
    print("Testing Open-Meteo API for Riverside, CA...")
    weather = fetch_live_weather(33.9533, -117.3961)
    if weather:
        print(json.dumps(weather, indent=2))
    else:
        print("Failed to fetch weather data.")
