# Artistic Intelligence data
Data collector, query API and more to make digital art. 

Check out our work at https://artisticintelligence.nl/!

Note: the Python package is named `aid`, because I found `artistic_intelligence_data` a bit too verbose.

## Setting up

### Python environment
This package is managed by Poetry. Install it with `pip install poetry` if you don't already have it.
Then it's simply `poetry install --with dev` to set up your development environment. 
Using a virtual environment is recommended.

### Database
This project currently uses PostgreSQL with the PostGIS and TimescaleDB extensions for storage.

I recommend setting up on a Linux environment (WSL if you're on Windows). Follow the guides:
- Basic PostgreSQL: https://www.postgresql.org/download/linux/ubuntu/
  (manually configure the apt repository to get the latest version)
- PostGIS comes with the above repository, so `sudo apt install postgresql-postgis` should work.
- TimescaleDB: https://docs.timescale.com/self-hosted/latest/install/installation-linux/

After setting up a database and users, you can use the SQL scripts in the `ddl` folder
to set up raw storage tables for each data source.

## Developing
This project uses `SCons` to standardize quality steps. Remember to run `scons all` before you commit!
This does code fixing, formatting, then quality checks. The GitHub Actions pipeline runs the same quality checks.

## Configuration
The application uses `python-dotenv` to load environment variables from the `.env` file. 
You can also pass your own environment variables when running the application.