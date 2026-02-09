require 'sinatra'
require 'fast_mcp'
require 'http'
require 'json'
require 'logger'

#set :logger, Logger.new(STDOUT)
#set :bind, '0.0.0.0'
server = MCP::Server.new(name: 'solis-mcp-server', version: '0.0.1')

class SearchTool < MCP::Tool
  description "SOLIS search engine"

  arguments do
    required(:query).filled(:string).description("Query in Lucene like (index: ((term+|phrase+) operator? (term+|phrase+)?)*)+")
  end

  def call(query)
    result = nil
    response = HTTP.get("https://services.libis.be/search?query=#{:query}")
    if response.ok?
      result = response.parse
    end

    result
  end

  class << self
    attr_accessor :server
  end
end

server.register_tool(SearchTool)

use MCP::Transports::RackTransport, server


get '/' do
  'solis-mcp-server'
end


