#!/usr/bin/env ruby
$LOAD_PATH << './lib' <<'.'
require 'json'
require 'http'
require 'mcp'
require 'lib/config_file'
require 'lib/elastic/query'

module SolisClient
  def elastic_config
    @elastic_config ||= ConfigFile[:services][$SERVICE_ROLE][:elastic]
  end

  def self.get_indexes
    indexes = Query::Indexes.new(elastic_config[:templates][:mapping])

    indexes_list = {}
    indexes_list = indexes_list.merge(indexes.index_map, indexes.facet_map, indexes.query_mapping).map do |k, v|
      if v.is_a?(Array) && v.length > 0 && v[0].eql?('id')
        { k => '1234' }
      elsif v.is_a?(Array)
        { k => '' }
      elsif (v.is_a?(Hash) && v.keys.first.eql?('{{}}'))
        { k => '' }
      elsif v.is_a?(Hash)
        { k => v.keys.first }
      else
        { k => '' }
      end
    end

    indexes_list
  end


end

name "solis-mcp-server"
version "0.0.1"

resource "solis://search" do
  name "ODIS Search Engine Help"
  description "Help page for the ODIS search engine using Lucene syntax."
  call {"ODIS Search Engine Help"}
end

tool "solis://search" do
  description "Perform a search using Lucene syntax."
  argument :query, String, required: true, description: "A query using the Lucene syntax"
  argument :bulkSize, Integer, required: false, description: "Number of records to return, defaults to 10"
  argument :from, Integer, required: false, description: "Return records starting from this index"
  argument :from_base, Integer, required: false, description: "Specifies whether the index is 0-based or 1-based"
  argument :sort, String, required: false, description: "The index to sort results by"

  call do |args|
    pp args
    {data: []}
  end
end

tool "solis://get_indexes" do
  description "Returns all available indexes"
  call {
    [
      "id",
      "naam",
      "record_type",
      "type",
      "any",
      "titel",
      "facet_rid",
      "facet_organisatie",
      "facet_organisatie_type",
      "facet_record_type",
      "all",
      "ac_organisatie"
    ]
  }
end

tool "solis://get_sort_indexes" do
  description "Returns all available sort indexes. Indexes starting with an 'a' are ascending and with a 'd' descending."
  call {
    [
      "aNaam",
      "dNaam",
      "aRelevantie",
      "dRelevantie",
      "aChrono",
      "dChrono",
      "aCategorie",
      "dCategorie"
    ]
  }
end
