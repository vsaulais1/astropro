from __future__ import annotations

import json
import math
import os
import time
import typing as t
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

# Spotify
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler, CacheFileHandler


# ------------------------------
# Config (tweak to your liking)
# ------------------------------
LOCAL_TZ = "Europe/Paris"  # FIP/Radio France local time
RUN_TZ = pendulum.timezone(LOCAL_TZ)

OUTPUT_DIR = "/tmp/fip_groove"   # Airflow worker local FS
OUTPUT_CSV = "FIP_GROOVE.csv"    # written per run under OUTPUT_DIR/run_id/

#RADIOFRANCE_API = "https://openapi.radiofrance.fr/v1/graphql"
RADIOFRANCE_STATION = "FIP_GROOVE"

# Playlist naming scheme
PLAYLIST_NAME_TEMPLATE = "My {month_name} FIP Groove Playlist"  # e.g., "My October FIP Groove Playlist"
PLAYLIST_DESCRIPTION = (
    "A selection of tracks played on FIP Groove – Playlist generated automatically via Radio France API and Spotify."
)

# Spotify add-tracks batch size
SPOTIFY_BATCH_SIZE = 100


def _ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _yesterday_unix_range_paris(now: datetime) -> tuple[int, int, str]:
    """
    Compute yesterday's [start, end] Unix timestamps for Europe/Paris, and return month name for playlist naming.
    """
    dt_now = RUN_TZ.convert(pendulum.instance(now))
    y_start = dt_now.subtract(days=1).start_of("day")
    y_end = dt_now.subtract(days=1).end_of("day")
    return int(y_start.timestamp()), int(y_end.timestamp()), y_start.format("MMMM")


def _to_artists_str(val: t.Any) -> str:
    if isinstance(val, list):
        return ", ".join([str(a) for a in val if a is not None])
    if pd.isna(val):
        return ""
    return str(val)


def _chunked(seq: list[str], n: int) -> list[list[str]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]

def _get_spotify_conn():
    try:
        return BaseHook.get_connection("spotify")
    except Exception as e:
        raise AirflowFailException(f"Missing or misconfigured Airflow Connection 'spotify_api': {e}")

def _get_spotify_app_creds():
    conn = _get_spotify_conn()
    client_id = conn.login
    client_secret = conn.password
    if not client_id or not client_secret:
        raise AirflowFailException(
            "Airflow Connection 'spotify_api' must have Login (client_id) and Password (client_secret)."
        )
    return client_id, client_secret, (conn.extra_dejson or {})


# -------------
# The DAG
# -------------
@dag(
    dag_id="fip_groove_to_spotify",
    description="Build a monthly FIP Groove playlist from Radio France plays and push to Spotify.",
    schedule="@daily",                  # runs daily; each run processes *yesterday*
    start_date=pendulum.datetime(2025, 10, 1, tz=LOCAL_TZ),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["radiofrance", "spotify", "music", "fip"],
)
def fip_groove_to_spotify_dag():

    @task()
    def compute_time_window() -> dict:
        start_ts, end_ts, month_name = _yesterday_unix_range_paris(datetime.utcnow())
        return {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "month_name": month_name,
        }

    @task()
    def fetch_radiofrance(window: dict) -> dict:
        """
        Calls Radio France GraphQL for FIP_GROOVE grid for yesterday.
        Uses Airflow Connection: radiofrance_api
          - Extras JSON: {"headers": {"x-token": "YOURTOKEN"}}
          - Host: openapi.radiofrance.fr
          - Schema: https
        """
        # Validate connection & headers
        try:
            conn = BaseHook.get_connection("radiofrance_api")
        except Exception:
            raise AirflowFailException("Missing Airflow Connection: radiofrance_api")

        extras = conn.extra_dejson or {}
        extra_headers = {"Content-Type": "application/json", "x-token": extras.get("x-token")}
        if not extra_headers.get("x-token"):
            raise AirflowFailException("Connection 'radiofrance_api' is missing Extras.headers.x-token")

        start_ts = window["start_ts"]
        end_ts = window["end_ts"]

        query = f"""
        {{
          grid(start: {start_ts}, end: {end_ts}, station: {RADIOFRANCE_STATION}, includeTracks: true) {{
            ... on TrackStep {{
              id
              track {{
                id
                title
                albumTitle
                mainArtists
                productionDate
              }}
            }}
          }}
        }}
        """

        http = HttpHook(method="POST", http_conn_id="radiofrance_api")
        headers = {"Content-Type": "application/json", **extra_headers}
        resp = http.run(
            endpoint="/v1/graphql",
            data=json.dumps({"query": query}),
            headers=headers,
            extra_options={"timeout": 60},
        )

        try:
            resp.raise_for_status()
        except Exception as e:
            raise AirflowFailException(f"Radio France API error: {e} - body: {resp.text[:500]}")

        payload = resp.json()
        if "errors" in payload:
            raise AirflowFailException(f"GraphQL returned errors: {payload['errors']}")

        grid = payload.get("data", {}).get("grid", [])
        return {"grid": grid}


    @task()
    def transform_to_csv(api_result: dict, run_id: str) -> dict:
        """
        Normalizes the grid into a clean CSV and stores it under /tmp/fip_groove/<run_id>/FIP_GROOVE.csv.
        Returns the path and a minimal list of search dicts for the next step.
        """
        grid = api_result.get("grid", [])
        df = pd.json_normalize(grid)

        if df.empty:
            # Still write an empty CSV for observability
            out_dir = _ensure_dir(Path(OUTPUT_DIR) / run_id)
            out_csv = out_dir / OUTPUT_CSV
            df.to_csv(out_csv, index=False)
            return {"csv_path": str(out_csv), "search_items": []}

        # Expected columns after normalize (guard with .get)
        # track.title, track.albumTitle, track.mainArtists, track.productionDate
        df["Artists"] = df["track.mainArtists"].apply(_to_artists_str)

        # Clean up and reorder
        cols = [
            "track.id",
            "track.title",
            "Artists",
            "track.albumTitle",
            "track.productionDate",
            "id",  # TrackStep id
        ]
        existing_cols = [c for c in cols if c in df.columns]
        clean_table = df[existing_cols].drop_duplicates()

        # Write CSV artifact
        out_dir = _ensure_dir(Path(OUTPUT_DIR) / run_id)
        out_csv = out_dir / OUTPUT_CSV
        clean_table.to_csv(out_csv, index=False)

        # Build search records for Spotify
        search_items: list[dict] = []
        for _, row in clean_table.iterrows():
            title = str(row.get("track.title", "")).strip()
            artists = str(row.get("Artists", "")).strip()
            prod_date = str(row.get("track.productionDate", "")).strip()

            # If productionDate looks like a year, include it in the query with a filter
            year_filter = ""
            if prod_date and prod_date.isdigit():
                year_filter = f" year:{prod_date}"

            query = f"{title} {artists}{year_filter}".strip()

            if title or artists:
                search_items.append(
                    {"query": query, "row_track_id": row.get("track.id", None)}
                )

        return {"csv_path": str(out_csv), "search_items": search_items}

    @task()
    def get_spotify_uris(search_payload: dict) -> list[str]:
        """
        Uses client credentials to search Spotify for each track and collect URIs.
        Pulls client_id/client_secret from Airflow Connection 'spotify_api'.
        """
        client_id, client_secret, _extras = _get_spotify_app_creds()

        sp = spotipy.Spotify(
            client_credentials_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            ),
            requests_timeout=20,
            retries=3,
        )

        uris: list[str] = []
        for idx, rec in enumerate(search_payload.get("search_items", []), start=1):
            q = rec["query"]
            try:
                res = sp.search(q=q, type="track", limit=1)
                items = res.get("tracks", {}).get("items", [])
                if items:
                    uris.append(items[0]["uri"])
            except spotipy.SpotifyException as se:
                if getattr(se, "http_status", None) == 429:
                    retry_after = int(getattr(se, "headers", {}).get("Retry-After", 1))
                    time.sleep(retry_after + 1)
                    continue
                print(f"[WARN] Spotify search failed for '{q}': {se}")
            except Exception as e:
                print(f"[WARN] Unexpected error for '{q}': {e}")

            if idx % 10 == 0:
                time.sleep(0.2)

        # de-dupe preserving order
        seen = set()
        uniq = []
        for u in uris:
            if u not in seen:
                uniq.append(u)
                seen.add(u)

        return uniq


# ---------- task 2: create playlist and add tracks via user OAuth ----------

    @task()
    def create_playlist_and_add_tracks(uris: list[str], window: dict) -> dict:
        """
        Creates (or reuses) the monthly playlist and adds missing tracks in batches.
        Uses Airflow Connection 'spotify_api':
          - Login: client_id
          - Password: client_secret
          - Extras JSON:
              {
                "redirect_uri": "http://127.0.0.1:8888/callback",
                "username": "your_spotify_user_id",
                "scope": "playlist-modify-public",
                "refresh_token": "...."   # <-- add this after one-time auth
              }
        """
        if not uris:
            return {"message": "No URIs found; nothing to add."}

        client_id, client_secret, extras = _get_spotify_app_creds()
        redirect_uri = extras.get("redirect_uri")
        username = extras.get("username")
        scope = extras.get("scope", "playlist-modify-public")
        refresh_token = extras.get("refresh_token")  # optional but recommended in Airflow

        if not all([redirect_uri, username]):
            raise AirflowFailException(
                "Airflow Connection 'spotify_api' Extras must include 'redirect_uri' and 'username'."
            )

        # ---- Spotipy OAuth cache: ensure it's a readable file, not a dir ----
        cache_dir = "/tmp/spotipy_cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, f".cache-{username}.json")
        if os.path.isdir(cache_path):
            os.rename(cache_path, cache_path + ".bak_dir")
        if not os.path.exists(cache_path):
            open(cache_path, "w").close()
        try:
            with open(cache_path, "r+"):
                pass
        except Exception as e:
            raise AirflowFailException(
                f"OAuth cache file not readable/writable: {cache_path} ({e})"
            )

        # ---- Build a cache handler:
        # If a refresh_token is present, seed a Memory cache so no browser/STDIN is required.
        cache_handler = None
        if refresh_token:
            token_info_seed = {
                "refresh_token": refresh_token,
                "scope": scope,
                # force immediate refresh:
                "expires_at": 0,
            }
            cache_handler = MemoryCacheHandler(token_info=token_info_seed)
        else:
            # Fallback to file cache (may still prompt if no prior token exists!)
            cache_handler = CacheFileHandler(cache_path=cache_path)

        auth_manager = SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,  # must match the one used to generate the refresh_token
            scope=scope,
            open_browser=False,         # never try to open browser on workers
            cache_handler=cache_handler,
            # NOTE: do NOT set 'username' here; Spotipy ignores it in modern flows
        )

        # Ensure we have an access token (this will silently refresh if refresh_token is present)
        try:
            _ = auth_manager.get_access_token(as_dict=True, check_cache=True)
        except Exception as e:
            raise AirflowFailException(
                "Spotify auth requires user interaction and no refresh_token was provided. "
                "Add 'refresh_token' to the 'spotify_api' connection Extras or pre-populate the cache file. "
                f"Underlying error: {e}"
            )

        sp = spotipy.Spotify(auth_manager=auth_manager, requests_timeout=30, retries=3)

        month_name = window["month_name"]
        playlist_name = PLAYLIST_NAME_TEMPLATE.format(month_name=month_name)

        # Find existing playlist (first exact name match owned by the user)
        playlist_id = None
        results = sp.current_user_playlists(limit=50)
        while True:
            for pl in results.get("items", []):
                if pl["name"] == playlist_name and pl["owner"]["id"] == username:
                    playlist_id = pl["id"]
                    break
            if playlist_id or not results.get("next"):
                break
            results = sp.next(results)

        # Create if missing
        if not playlist_id:
            pl = sp.user_playlist_create(
                user=username,
                name=playlist_name,
                public=("private" not in scope),  # heuristic: private if scope contains 'private'
                collaborative=False,
                description=PLAYLIST_DESCRIPTION,
            )
            playlist_id = pl["id"]

        # Get current tracks to avoid duplicates
        existing_uris = set()
        tracks_page = sp.playlist_items(
            playlist_id,
            fields="items(track(uri)),next",
            additional_types=["track"]
        )
        while True:
            for it in tracks_page.get("items", []):
                track = it.get("track")
                if track and track.get("uri"):
                    existing_uris.add(track["uri"])
            if not tracks_page.get("next"):
                break
            tracks_page = sp.next(tracks_page)

        # Compute to-add (preserve incoming order)
        to_add = [u for u in uris if u not in existing_uris]
        added_total = 0

        for chunk in _chunked(to_add, SPOTIFY_BATCH_SIZE):
            sp.playlist_add_items(playlist_id, chunk)
            added_total += len(chunk)
            time.sleep(0.2)

        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "added": added_total,
            "skipped_existing": len(uris) - added_total,
        }


    # Orchestrate
    window = compute_time_window()
    rf = fetch_radiofrance(window)
    transformed = transform_to_csv(rf, run_id="{{ run_id }}")
    uris = get_spotify_uris(transformed)
    summary = create_playlist_and_add_tracks(uris, window)

fip_groove_to_spotify_dag()
