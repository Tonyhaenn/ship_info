# Ship Info

This project is a simple Elixir script that uses the Perplexity API to find information about ships.

## Usage

1. Download a BOL file from ImportGenius. We expect a CSV with at least the following columns:
- VESSEL NAME
- QUANTITY UNIT
- CARRIER CODE
- CARRIER NAME

2. Run the script:
```
elixir find_ship_info.exs
```

3. The script will output a CSV file with the ship information.
- vessel_name
- ship_type
- ship_registration_country
- ship_carrier_name
- ship_carrier_code
- imo_number
- ship_flag
- country_of_construction
- shipbuilder_name
- year_built
- lookup_status
- raw_response