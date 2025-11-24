require 'sinatra/base'
require 'http'
require 'json'
require 'http/accept'
require 'lib/config_file'
require 'app/helpers/main_helper'
require 'lib/elastic/query'

class MainController < Sinatra::Base
  helpers Sinatra::MainHelper

  configure do
    set :method_override, true # make a PUT, DELETE possible with the _method parameter
    set :show_exceptions, false
    set :raise_errors, false
    set :root, File.absolute_path("#{File.dirname(__FILE__)}/../../")
    set :views, (proc { "#{root}/app/views" })
    set :logging, true
    set :static, true
    set :public_folder, "#{root}/public"
  end

  before do
    accept_header = request.env['HTTP_ACCEPT']
    accept_header = params['accept'] if params.include?('accept')
    accept_header = 'text/html' if accept_header.nil?

    media_types = HTTP::Accept::MediaTypes.parse(accept_header).map { |m| m.mime_type.eql?('*/*') ? 'application/json' : m.mime_type } || ['application/json']
    @media_type = media_types.first

    content_type @media_type
  end

  get '/' do
    content_type :json
    halt 500, ERB::Util.html_escape('I need a "query" parameter') unless params.key?('query')
    query = params['query']
    query = "*" if query.nil? || query.empty? || query.blank?

    parser = Query::Parser.new(elastic_config[:templates])
    elastic_query = parser.parse(query).to_elastic(params)
    puts "\nORIGINAL_QUERY:\n#{query}"
    puts "PARSED QUERY:\n#{parser.parsed.to_json}"
    puts "ELASTIC_QUERY: \n#{elastic_query.to_json}"

    response = HTTP.post("#{elastic_config[:host]}/#{elastic_config[:index]}/_search", json: elastic_query)

    if response.status == 200
      in_params = {query: elastic_query, highlight: params['highlight'] || '0'}
      result = normalize_output(::JSON.parse(response.body.to_s), in_params)
    else
      logger.error(response.body.to_s)
      halt 500, ERB::Util.html_escape('Error doing search')
    end
    result.raw.to_json

  end

  get '/help' do
    words = ['water', 'wijn', 'boek', 'koning', 'antw', 'dirk', 'gebouw', 'open']

    if @media_type.eql?('application/json')
      available_indexes.to_json
    else
      erb :'help.html', locals: {base_path: ConfigFile[:services][$SERVICE_ROLE][:base_path], indexes: available_indexes, words: words}
    end

  end

  post '/' do
    halt 500, ERB::Util.html_escape('I only accept GET requests')
  end

  put '/' do
    halt 500, ERB::Util.html_escape('I only accept GET requests')
  end


  get '/ping' do
    content_type :json
    {
      "api": true,
      "storage": elastic?
    }.to_json
  end

  delete '/' do
    halt 500, ERB::Util.html_escape('I only accept GET requests')
  end

  not_found do
    content_type :json
    message = body
    logger.error(message)
    message.to_json
  end

  error do
    content_type :json
    message = { status: 500, body: "error:  #{env['sinatra.error'].to_s}" }
    logger.error(message)
    message.to_json
  end
end