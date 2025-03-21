# Simple script to lookup ship info using the Perplexity API

Mix.install([
  {:req, "~> 0.5.9"},
  {:csv, "~> 3.2"},
  {:dotenvy, "~> 1.0.1"}
])

defmodule ShipInfo do
  Dotenvy.source!([
    ".env",
    System.get_env()
  ])

  @perplexity_api_key Dotenvy.env!("PERPLEXITY_API_KEY", :string)

  @type ship_info :: %{
    vessel_name: String.t(),
    ship_type: String.t(),
    ship_registration_country: String.t(),
    ship_carrier_name: String.t(),
    ship_carrier_code: String.t()
  }
  @type enhanced_ship_info :: %{
          vessel_name: String.t(),
          ship_type: String.t(),
          ship_registration_country: String.t(),
          ship_carrier_name: String.t(),
          ship_carrier_code: String.t(),
          imo_number: String.t(),
          country_of_construction: String.t(),
          shipbuilder_name: String.t(),
          year_built: String.t(),
          ship_flag: String.t(),
          lookup_status: String.t(),
          raw_response: String.t()
        }

  @requests_per_minute 50
  # milliseconds between requests
  @delay_between_requests div(60_000, @requests_per_minute)

  @doc """
  Parse the ship CSV file and return a stream of shipment data

  ## Parameters
    - file_path: Path to the CSV file
    - rows: Number of rows to read (optional, defaults to :all)

  ## Examples
      # Read all rows
      parse_ship_csv("ships.csv")

      # Read only first 10 rows
      parse_ship_csv("ships.csv", 10)
  """
  @spec parse_ship_csv(String.t(), :all | pos_integer()) :: Stream.t(map())
  def parse_ship_csv(file_path, rows \\ :all) do
    stream =
      file_path
      |> File.stream!([], :line)
      |> CSV.decode(headers: true)
      |> Stream.map(fn {:ok, row} -> row end)

    case rows do
      :all -> stream
      n when is_integer(n) and n > 0 -> Stream.take(stream, n)
      _ -> raise ArgumentError, "rows must be :all or a positive integer"
    end
  end

  @doc """
  Transforms the the quantity unit into a container type code
  """
  @spec transform_quantity_unit_to_container_type_code(String.t()) :: String.t()
  def transform_quantity_unit_to_container_type_code(quantity_unit) do
    case quantity_unit do
      "LBK" -> "Tanker / Chemical Tanker"
      "DBK" -> "Dry Bulk"
      "CBC" -> "Dry Bulk"
      _ -> "Container"
    end
  end



  @doc """
  Build a list of unique ship names and IMO numbers from the shipment data.
  Returns a stream of maps with :vessel_name and :imo_number keys.
  Empty or invalid entries are filtered out.
  """

  @spec get_unique_ship_names(Stream.t(map())) :: Stream.t(ship_info())
  def get_unique_ship_names(ship_data_stream) do
    ship_data_stream
    |> Stream.map(fn row ->
      %{
        vessel_name: Map.get(row, "VESSEL NAME", ""),
        ship_type: transform_quantity_unit_to_container_type_code(Map.get(row, "QUANTITY UNIT", "")),
        ship_registration_country: Map.get(row, "SHIP REGISTERED IN", ""),
        ship_carrier_name: Map.get(row, "CARRIER NAME", ""),
        ship_carrier_code: Map.get(row, "CARRIER CODE", "")
      }
    end)
    |> Stream.reject(fn %{vessel_name: name} -> name == "" end)
    |> Stream.uniq_by(& &1.vessel_name)
  end

  @doc """
  Lookup ship country of construction using the Perplexity API.
  Rate limited to #{@requests_per_minute} requests per minute.
  """
  @spec lookup_ship_country_of_construction(Stream.t(ship_info())) ::
          Stream.t(enhanced_ship_info())
  def lookup_ship_country_of_construction(unique_vessel_stream) do
    unique_vessel_stream
    |> Stream.map(fn vessel ->
      IO.puts("Processing #{vessel.vessel_name}...")
      # Add delay before each API call to respect rate limit
      Process.sleep(@delay_between_requests)
      initial_ship_lookup(vessel)
    end)
  end

  @spec initial_ship_lookup(ship_info()) :: enhanced_ship_info()
  def initial_ship_lookup(vessel) do
    prompt = """
    I need to find the country of construction for the following ship:
    Vessel Name: #{vessel.vessel_name}
    Vessel Type: #{vessel.ship_type}
    Ship Registration Country: #{vessel.ship_registration_country}
    Ship Carrier Name: #{vessel.ship_carrier_name}
    Ship Carrier Code: #{vessel.ship_carrier_code}

    Please return the following information in JSON format according to this schema:
    {
      "type": "object",
      "properties": {
        "vessel_name": { "type": "string" },
        "imo_number": { "type": "string" },
        "country_of_construction": { "type": "string" },
        "shipbuilder_name": { "type": "string" },
        "ship_flag": { "type": "string" },
        "year_built": { "type": "string" }
      },
      "required": ["vessel_name", "imo_number", "country_of_construction", "shipbuilder_name", "ship_flag", "year_built"]
    }

    You must ONLY return a JSON object with the above fields. No other text or comments.
    """

    response =
      Req.post!(
        "https://api.perplexity.ai/chat/completions",
        json: %{
          model: "sonar-pro",
          messages: [
            %{
              role: "system",
              content:
                "You are a helpful assistant that returns information about ships in JSON format."
            },
            %{
              role: "user",
              content: prompt
            }
          ],
          temperature: 0.8,
          web_search_options: %{search_context_size: "medium"}
        },
        headers: [
          Authorization: "Bearer #{@perplexity_api_key}",
          "Content-Type": "application/json"
        ]
      )

    # Parse the response and convert to our enhanced_ship_info format
    case response.body do
      %{"choices" => [%{"message" => %{"content" => content}} | _]} ->
        case JSON.decode(content) do
          {:ok, ship_info} ->
            # Check if we got an IMO but missing construction details
            if ship_info["imo_number"] not in [nil, ""] && ship_info["country_of_construction"] in [nil, "", "Unknown"] do
              Process.sleep(@delay_between_requests)  # Add delay before second lookup
              second_lookup_result = lookup_with_imo(vessel.vessel_name, ship_info["imo_number"])

              # Merge results, preferring second lookup for construction details
              %{
                vessel_name: vessel.vessel_name,
                ship_type: vessel.ship_type,
                ship_registration_country: vessel.ship_registration_country,
                ship_carrier_name: vessel.ship_carrier_name,
                ship_carrier_code: vessel.ship_carrier_code,
                imo_number: ship_info["imo_number"],
                ship_flag: ship_info["ship_flag"],
                country_of_construction: second_lookup_result["country_of_construction"] || ship_info["country_of_construction"],
                shipbuilder_name: second_lookup_result["shipbuilder_name"] || ship_info["shipbuilder_name"],
                year_built: ship_info["year_built"],
                lookup_status: "success_with_retry",
                raw_response: ""
              }
            else
              # Use original successful result
              %{
                vessel_name: vessel.vessel_name,
                ship_type: vessel.ship_type,
                ship_registration_country: vessel.ship_registration_country,
                ship_carrier_name: vessel.ship_carrier_name,
                ship_carrier_code: vessel.ship_carrier_code,
                imo_number: ship_info["imo_number"],
                ship_flag: ship_info["ship_flag"],
                country_of_construction: ship_info["country_of_construction"],
                shipbuilder_name: ship_info["shipbuilder_name"],
                year_built: ship_info["year_built"],
                lookup_status: "success",
                raw_response: ""
              }
            end
          {:error, _} ->
            %{
              vessel_name: vessel.vessel_name,
              ship_type: vessel.ship_type,
              ship_registration_country: vessel.ship_registration_country,
              ship_carrier_name: vessel.ship_carrier_name,
              ship_carrier_code: vessel.ship_carrier_code,
              imo_number: "",
              ship_flag: "",
              country_of_construction: "",
              shipbuilder_name: "",
              year_built: "",
              lookup_status: "fail",
              raw_response: content
            }
        end

      _ ->
        %{
          vessel_name: vessel.vessel_name,
          ship_type: vessel.ship_type,
          ship_registration_country: vessel.ship_registration_country,
          ship_carrier_name: vessel.ship_carrier_name,
          ship_carrier_code: vessel.ship_carrier_code,
          imo_number: "",
          ship_flag: "",
          country_of_construction: "",
          shipbuilder_name: "",
          year_built: "",
          lookup_status: "api_error",
          raw_response: inspect(response.body)
        }
    end
  end

  @spec lookup_with_imo(String.t(), String.t()) :: map()
  def lookup_with_imo(vessel_name, imo_number) do
    prompt = """
    For the following ship:
    Vessel Name: #{vessel_name}
    IMO Number: #{imo_number}

    Can you lookup the country of construction and ship builder?

    Please return the following information in JSON format according to this schema:
    {
      "type": "object",
      "properties": {
        "country_of_construction": { "type": "string" },
        "shipbuilder_name": { "type": "string" }
      },
      "required": ["country_of_construction", "shipbuilder_name"]
    }

    You must ONLY return a JSON object with the above fields. No other text or comments.
    """

    response =
      Req.post!(
        "https://api.perplexity.ai/chat/completions",
        json: %{
          model: "sonar-pro",
          messages: [
            %{
              role: "system",
              content: "You are a helpful assistant that returns information about ships in JSON format."
            },
            %{
              role: "user",
              content: prompt
            }
          ],
          temperature: 0.8,
          web_search_options: %{search_context_size: "medium"}
        },
        headers: [
          Authorization: "Bearer #{@perplexity_api_key}",
          "Content-Type": "application/json"
        ]
      )

    case response.body do
      %{"choices" => [%{"message" => %{"content" => content}} | _]} ->
        case JSON.decode(content) do
          {:ok, ship_info} -> ship_info
          {:error, _} -> %{"country_of_construction" => "", "shipbuilder_name" => ""}
        end
      _ ->
        %{"country_of_construction" => "", "shipbuilder_name" => ""}
    end
  end

  @doc """
  Write enhanced ship info to a CSV file.
  Returns the number of records written.
  """
  @spec write_enhanced_ship_info(Stream.t(enhanced_ship_info()), Path.t()) :: non_neg_integer()
  def write_enhanced_ship_info(enhanced_ship_stream, output_path) do
    headers = [
      "vessel_name",
      "ship_type",
      "ship_registration_country",
      "ship_carrier_name",
      "ship_carrier_code",
      "imo_number",
      "ship_flag",
      "country_of_construction",
      "shipbuilder_name",
      "year_built",
      "lookup_status",
      "raw_response"
    ]

    # Create fresh file and write headers
    file = File.open!(output_path, [:write, :utf8])
    :ok = CSV.encode([headers]) |> Enum.each(&IO.write(file, &1))

    # Write all ships and count them
    count =
      Enum.reduce(enhanced_ship_stream, 0, fn ship, acc ->
        # Ensure we're passing a list to CSV.encode
        [
          [
            ship.vessel_name,
            ship.ship_type,
            ship.ship_registration_country,
            ship.ship_carrier_name,
            ship.ship_carrier_code,
            ship.imo_number,
            ship.ship_flag,
            ship.country_of_construction,
            ship.shipbuilder_name,
            ship.year_built,
            ship.lookup_status,
            ship.raw_response
          ]
        ]
        |> CSV.encode()
        |> Enum.each(&IO.write(file, &1))

        acc + 1
      end)

    File.close(file)
    count
  end
end

# Main script execution
output_file = "enhanced_ship_info_#{Date.utc_today()}.csv"

"us_imports_by_vessel_03182025.csv"
|> ShipInfo.parse_ship_csv(:all)
|> ShipInfo.get_unique_ship_names()
|> ShipInfo.lookup_ship_country_of_construction()
|> ShipInfo.write_enhanced_ship_info(output_file)
|> then(&IO.puts("Successfully processed #{&1} ships and wrote results to #{output_file}"))
