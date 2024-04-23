# Artistic Intelligence data
Data collector, query API and more to make digital art. 

Check out our work at https://artisticintelligence.nl/!

### Notes
- This package is managed by Poetry. Install it with `pip install poetry` if you don't already have it.
  Then it's simply `poetry install --with dev` to set up your development environment. 
  Using a virtual environment is recommended.
- The Python package is called `aid`, because `artistic_intelligence_data` is a bit verbose.
- The `ddl` folder contains SQL statements to set up the database. 
  This is aimed at PostgreSQL with the PostGIS and TimescaleDB extensions.
- Configure the python app using environment variables or the `.env` file.
- This project uses `SCons` to standardize quality steps. Remember to run `scons all` before you commit!