import pendulum

# ------------------------------
# Config
# ------------------------------

class fiptify_config:
    def __init__(self):
        self.GCP_CONN_ID = "google_default"
        self.LOCAL_TZ = "Europe/Paris"  # FIP/Radio France local time
        self.RUN_TZ = pendulum.timezone(self.LOCAL_TZ)

        self.OUTPUT_DIR = "/tmp/fip_groove"   # Airflow worker local FS
        self.OUTPUT_CSV = "FIP_GROOVE.csv"    # written per run under OUTPUT_DIR/run_id/

        #RADIOFRANCE_API = "https://openapi.radiofrance.fr/v1/graphql"
        self.RADIOFRANCE_STATION = "FIP_GROOVE"

        # Playlist naming scheme
        self.PLAYLIST_NAME_TEMPLATE = "My {month_name} FIP Groove Playlist"  # e.g., "My October FIP Groove Playlist"
        self.PLAYLIST_DESCRIPTION = (
            "A selection of tracks played on FIP Groove – Playlist generated automatically via Radio France API and Spotify."
        )

        # Spotify add-tracks batch size
        self.SPOTIFY_BATCH_SIZE = 100

