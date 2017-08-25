defmodule ExAws.Athena do
  @moduledoc """
  Operations on the AWS Athena service.
  """

  @namespace "AmazonAthena"

  @type query_execution_id :: binary

  @doc """
    named_query_ids [list of strings | string]
  """
  def batch_get_named_query(named_query_ids) when is_binary(named_query_ids) do
    batch_get_named_query([named_query_ids])
  end
  def batch_get_named_query(named_query_ids) when length(named_query_ids) > 50 do
    throw({:error,
      "length of NamedQueryIds cannot exceed 50, got #{length(named_query_ids)}"
    })
  end
  def batch_get_named_query(named_query_ids) when is_list(named_query_ids) do
    request(:batch_get_named_query, %{
      "NamedQueryIds" => named_query_ids
    })
  end

  @doc """
    named_query_ids [list of strings | string]
  """
  def batch_get_query_execution(named_query_ids) when is_binary(named_query_ids) do
    batch_get_query_execution([named_query_ids])
  end
  def batch_get_query_execution(named_query_ids) when length(named_query_ids) > 50 do
    throw({:error,
      "length of QueryExecutionIds cannot exceed 50, got #{length(named_query_ids)}"
    })
  end
  def batch_get_query_execution(named_query_ids) do
    request(:batch_get_query_execution, %{
      "QueryExecutionIds" => named_query_ids
    })
  end

  @doc """
    name [string]
      query name
    db [string]
      athena database name
    query [string]
      query string
    description [string](optional)
      query description
    client_request_token [string]
      ...
  """
  def create_named_query(name, database, query, description \\ nil, client_request_token \\ generate_token()) do
    request(:create_named_query, %{
      "Name" => name,
      "Database" => database,
      "QueryString" => query,
      "Description" => description,
      "ClientRequestToken" => client_request_token
    })
  end

  @doc """
    named_query_id [string]
      id of query to be deleted
  """
  def delete_named_query(named_query_id) do
    request(:delete_named_query, %{
      "NamedQueryId" => named_query_id
    })
  end

  @doc """
    named_query_id [string]
      id of query to return
  """
  def get_named_query(named_query_id) do
    request(:get_named_query, %{
      "NamedQueryId" => named_query_id
    })
  end

  @doc """
    query_execution_id [string]
      id of query execution to return
  """
  def get_query_execution(query_execution_id) do
    request(:get_query_execution, %{
      "QueryExecutionId" => query_execution_id
    })
  end

  @doc """
    query_execution_id [string]
      execution id
    max_results [integer]
      max number of results, max 50
    next_token [string]
      pagination token for next page
  """
  def get_query_results(query_execution_id, max_results \\ 50, next_token \\ nil) do
    request(:get_query_results, %{
      "QueryExecutionId" => query_execution_id,
      "MaxResults" => max_results,
      "NextToken" => next_token
    })
  end

  @doc """
    max_results [integer] (optional)
      number of results to return, limit 50
    next_token [string] (optional)
      token for next group of results for pagination. returned in each request
  """
  def list_query_executions(max_results \\ 50, next_token \\ nil) do
    request(:list_query_executions, %{
      "MaxResults" => max_results,
      "NextToken" => next_token
    })
  end

  @doc """
    max_results [integer] (optional)
      number of results to return, limit 50
    next_token [string] (optional)
      token for next group of results for pagination. returned in each request
  """
  def list_named_queries(max_results \\ 50, next_token \\ nil) do
    request(:list_named_queries, %{
      "MaxResults" => max_results,
      "NextToken" => next_token
    })
  end

  @doc """
    database [string]
      database for query
    query [string]
      query string
    output_location [string]
      location in amazon s3 to store results
    encryption_configuration [map] (optional)
      see: http://docs.aws.amazon.com/athena/latest/APIReference/API_EncryptionConfiguration.html
  """
  def start_query_execution(database, query, output_location, encryption_configuration \\ nil, client_request_token \\ generate_token()) do
    request(:start_query_execution, %{
      "ClientRequestToken" => client_request_token,
      "QueryExecutionContext" => %{
        "Database" => database
      },
      "QueryString" => query,
      "ResultConfiguration" => %{
        "OutputLocation" => output_location,
        "EncryptionConfiguration" => encryption_configuration
      }
    })
  end

  @doc "Stop query execution"
  @spec stop_query_execution(query_execution_id :: query_execution_id) :: ExAws.Operation.JSON.t
  def stop_query_execution(query_execution_id) do
    request(:stop_query_execution, %{
      "QueryExecutionId" => query_execution_id
    })
  end

  # REQUEST
  defp request(op, data, opts \\ %{}) do
    operation =
      op
      |> Atom.to_string
      |> Macro.camelize

    ExAws.Operation.JSON.new(:athena, %{
      data: data,
      headers: [
        {"x-amz-target", "#{@namespace}.#{operation}"},
        {"content-type", "application/x-amz-json-1.0"},
      ]
    } |> Map.merge(opts))
  end

  def generate_token() do
    :crypto.strong_rand_bytes(24)
    |> Base.url_encode64()
  end
end
