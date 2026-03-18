# plugins/odds_api_client.py
import requests

BASE_URL = "https://api.the-odds-api.com/v4"


def fetch_events(api_key, sport, **extra_params):
    url = f"{BASE_URL}/sports/{sport}/events"
    params = {"apiKey": api_key}
    params.update(extra_params)
    response = requests.get(url, params=params)
    response.raise_for_status()
    remaining = int(response.headers.get("x-requests-remaining", 0))
    return response.json(), remaining


def fetch_odds(api_key, sport, regions, markets, bookmakers, odds_format="american"):
    url = f"{BASE_URL}/sports/{sport}/odds"
    response = requests.get(url, params={
        "apiKey": api_key,
        "regions": ",".join(regions),
        "markets": ",".join(markets),
        "bookmakers": ",".join(bookmakers),
        "oddsFormat": odds_format,
    })
    response.raise_for_status()
    remaining = int(response.headers.get("x-requests-remaining", 0))
    return response.json(), remaining


def fetch_player_props(api_key, sport, regions, markets, bookmakers, odds_format="american"):
    return fetch_odds(api_key, sport, regions, markets, bookmakers, odds_format)


def fetch_scores(api_key, sport, days_from=3):
    url = f"{BASE_URL}/sports/{sport}/scores"
    response = requests.get(url, params={
        "apiKey": api_key,
        "daysFrom": days_from,
    })
    response.raise_for_status()
    remaining = int(response.headers.get("x-requests-remaining", 0))
    return response.json(), remaining
