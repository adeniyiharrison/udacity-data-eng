# Capstone

## Steps
1. Create an app on [Spotify Developers](https://developers.spotify.com/) to retreive your ID and SECRET
2. Download Spotipy, a lightweight Python library for Spotify Web API.
    * `pip install spotipy`
    * `pip freeze | grep spotipy > requirements.txt`
3. Run docker container
    * https://towardsdatascience.com/getting-started-with-airflow-using-docker-cd8b44dbff98
    * https://stackoverflow.com/questions/56904783/how-to-install-a-python-library-to-a-pulled-docker-image
    * `docker build -t capstone_airflow .`
    * `docker run -d --rm -p 8080:8080 -v /Users/adeniyiharrison/Documents/github/udacity-data-eng/capstone/dag:/usr/local/airflow/dags capstone_airflow webserver`
4. Set up Redshift cluster
5. Set up variables and connections on Airflow UI
    * "aws_key", "aws_secret", "spotify_client_id", "spotify_client_secret"

