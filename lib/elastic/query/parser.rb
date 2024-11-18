$LOAD_PATH << '.'
require 'json'
require_relative 'indexes'
##
# Parser philosophy
# t=term
# o=operator
# i=index
#
# T=t(o?t?)+
# Q=(oiT)+
#

module Query
  class Parser
    attr_reader :parsed

    def initialize(templates)
      @templates = templates
      @mappings = templates[:mapping]
      @indexes = Indexes.new(@mappings)
      @date_range_relation = templates[:date_range_relation] || 'INTERSECTS'
      @parsed = []

    end

    def parse(query)
      @parsed = []
      # self.tokenize(query).normalize_rounded_brackets.normalize_terms.determine_phrases.determine_indexes.normalize_indexes.normalize_operators
      self.tokenize(query).determine_indexes.fix_tokenization.determine_phrases.remove_single_space.normalize_rounded_brackets.normalize_indexes.
        normalize_operators.normalize_rounded_brackets.normalize_terms
      self
    end

    def to_elastic(params = {})
      params = params.with_indifferent_access
      @parsed = reset_offset(@parsed)
      from_base = params[:from_base].to_i || 0
      sort_map = JSON.load(File.read(@mappings[:sort]))
      elastic_query = JSON.load(File.read(@templates[:query])).with_indifferent_access || {}

      elastic_query["from"] = params.key?(:from) ? ((params[:from].to_i - 1) < 0 ? (params[:from].to_i) : params[:from].to_i - 1) : from_base
      elastic_query["size"] = params[:bulksize] || params[:bulkSize] || 10
      elastic_query['sort'] = sort_map[params['sort']] if params.key?('sort') && !sort_map[params['sort']].nil?
      elastic_query["query"] = { "bool": {} } unless elastic_query.key?("query") && elastic_query["query"].key?("bool")

      # find all indexes
      qbl = query_bit_list().join()
      query_offset = []
      qbl.scan(/oi/) { |s| query_offset << $~.offset(0)[0] }

      query_offset.each_with_index do |offset, i|
        operator = @parsed[offset][:value]
        index = @parsed[offset + 1][:value]

        offset_until = @parsed.size
        offset_until = query_offset[i + 1] if i + 1 < query_offset.size

        query = @parsed[offset...offset_until]
        terms = reset_offset(query[2..].clone)
        from_index = 0
        internal_operators = terms.select { |s| s[:type].eql?('operator') }
        new_terms = {}
        if internal_operators.empty?
          new_terms[operator] = terms.map { |m| m[:value] }
        elsif !internal_operators.empty? && internal_operators.first[:value].eql?('NOT')
          internal_operators = [query.select { |s| s[:type].eql?('operator')}.select{|s| s[:value].eql?(operator)}.first] + internal_operators
        end

        internal_operators.each_with_index do |internal_operator, i|
          internal_operator_index = terms.rindex(internal_operator) || 0

          next_index = terms.rindex(internal_operators[i + 1]) || terms.length
          left = terms[from_index...internal_operator_index].map { |m| m[:value] }
          right = terms[internal_operator_index + 1...next_index].map { |m| m[:value] }

          if from_index==0 && internal_operator_index==0 && left.empty?
            left << terms[from_index][:value]
          end

          new_terms[internal_operator[:value]] = [] unless new_terms.key?(internal_operator[:value])
          new_terms[internal_operator[:value]] += (left + right)
          new_terms[internal_operator[:value]].uniq!

          from_index = internal_operator_index + 1
        end
        fragments = []
        new_terms.each do |sub_operator, terms|
          fragments << build_query_fragment(index, sub_operator, operator, terms)
        end

        eq = elastic_query["query"]["bool"]
        # is it a filter?
        if facet?(index)
          elastic_query["query"]["bool"] = elastic_query["query"]["bool"].merge({ "filter": { "bool": {} } }) unless elastic_query["query"]["bool"].key?("filter")

          fragments.each do |fragment|
            key = fragment.keys.first
            eq = elastic_query["query"]["bool"]["filter"]["bool"]
            unless key.eql?(elastic_operator(operator))
              unless elastic_query["query"]["bool"]["filter"]["bool"][elastic_operator(operator)]
                elastic_query["query"]["bool"]["filter"]["bool"] = elastic_query["query"]["bool"]["filter"]["bool"].merge({ "#{elastic_operator(operator)}" => { "bool": {} } })

                if elastic_query["query"]["bool"]["filter"]["bool"][elastic_operator(operator)].is_a?(Array)
                  elastic_query["query"]["bool"]["filter"]["bool"][elastic_operator(operator)] += { "bool" => {} }
                end
                eq = elastic_query["query"]["bool"]["filter"]["bool"][elastic_operator(operator)]["bool"]
              end
            end


            if eq[key].nil? && !elastic_operator(operator).eql?(key)
              queries = eq[elastic_operator(operator)].clone || []
              queries = [queries] unless queries.is_a?(Array)

              if fragment.is_a?(Hash)
                queries << {"bool": fragment}
              else
              # if key.eql?(fragment.keys.first)
              #   queries << fragment[key]
              # else
                queries << fragment
              # end
              end
            else
              queries = eq[key].clone || []
              if queries.is_a?(Hash) && queries.keys.include?('bool')
                queries = queries['bool'][key] || []
              end


              queries << fragment[key]
            end

            if elastic_query["query"]["bool"]["filter"]["bool"].keys.first.eql?(elastic_operator(operator))
              elastic_query["query"]["bool"]["filter"]["bool"][elastic_operator(operator)]=queries.flatten
            else
              eq[elastic_operator(operator)] = queries.flatten
            end
          end

        else
          fragments.each do |fragment|
            key = fragment.keys.first
            queries = eq[key] || []
            queries << fragment[key]

            eq[key] = queries.flatten
          end
        end

      end

      elastic_query
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      log_error(e.backtrace.join("\n"), __method__.to_s, __LINE__)
      self
    end

    def build_query_fragment(index, operator, index_operator, terms)
      sub_operator = operator
      query_terms = terms
      fragment = ''
      strategy_type = 'default'
      #  eq['bool'][elastic_operator(operator)] = [] unless eq['bool'].key?(elastic_operator(operator))

      # this is a special case. When the index is a query_mapped index and the query is a child of the query_map
      # ex. when the index is facet_newrecords and it's key is "07 days back"
      if @indexes.query_mapping.has_key?(index) && @indexes.query_mapping[index].has_key?(query_terms.flatten.first) # @indexes.query_mapping[index].has_key?(query)
        strategy_type = 'query_mapping_1'
        fragment = @indexes.query_mapping[index][query_terms.flatten.first]
        # same as previous but a key of the query_mapping = {{}} then this will be replaced with the query_terms
      elsif @indexes.query_mapping.has_key?(index) && @indexes.query_mapping[index].has_key?("{{}}")
        strategy_type = 'query_mapping_2'
        q = @indexes.query_mapping[index]["{{}}"]

        fragment = JSON.parse(q.to_json.gsub('{{}}', query_terms.flatten.first))
        # when index if a FACET we add a term index
      elsif facet?(index)
        strategy_type = 'facet'
        elastic_index = @indexes.facet_map[index]
        elastic_index = [elastic_index] unless elastic_index.is_a?(Array)
        fragment = []
        elastic_index.each do |ri|
          query_terms.each do |term|
            if ri =~ /date|duration/
              fragment << process_date(ri, term)
            else

              qs = JSON.load(File.read(@mappings[:term_query]))
              qs['term'][ri] = qs['term'].delete 'index'
              qs['term'][ri]['value'] = term.gsub(/^"/, '').gsub(/"$/, '').gsub('_PC_TN', '') #&.gsub(/([\+\-\=\&\|\>\<\!\(\)\{\}\[\]^\~:\/])/, '\\\\\1') || ''

              fragment << qs
            end
          end
        end
        operator = sub_operator unless operator.eql?('NOT')
        # when the index contains a date keyword make it a date search
      elsif index =~ /date|duration/
        strategy_type = 'date'
        elastic_index = @indexes.facet_map[index]
        process_date(elastic_index, query_terms.join(' '))
      elsif index.eql?('rid')
        strategy_type = 'rid'
        elastic_index = @indexes.index_map[index]
        if query_terms.is_a?(Array)
          query_terms = query_terms.map { |m| m.gsub(/^"TN_/, '"') }
        else
          query_terms = query_terms.gsub(/^"TN_/, '"')
        end

        elastic_index = [elastic_index] unless elastic_index.is_a?(Array)
        elastic_index.each do |ri|
          qs = JSON.load(File.read(@mappings[:term_query]))

          qs['term'][ri] = qs['term'].delete 'index'
          qs['term'][ri]['value'] = query_terms.join(' ').gsub(/^"/, '').gsub(/"$/, '')
          fragment = qs
        end

      else
        strategy_type = 'default'
        # in all other cases we use a query_string
        elastic_index = @indexes.index_map[index]
        elastic_index = @indexes.index_map['any'] if elastic_index.nil?

        query_operator = operator.eql?('NOT') ? 'AND' : operator

        query_string = JSON.load(File.read(@mappings[:query_string]))
        query_string['query_string']['default_operator'] = query_operator
        query_string['query_string']['fields'] = elastic_index
        query_string['query_string']['query'] = query_terms.join(' ')&.gsub(/([\+\-\=\&\|\>\<\!\(\)\{\}\[\]^\~:\/])/, '\\\\\1')&.gsub(/(\?)$/, '\\\\\1') || ''

        fragment = query_string
      end

      qf = {}
      if strategy_type.eql?('default') && !operator.eql?('NOT')
        qf[elastic_operator(index_operator)] = fragment
      else
        qf[elastic_operator(operator)] = fragment
      end
      qf
    end

    def tokenize(query)
      query.gsub!('\\', '')
      # query.scan(/\w+|\W(?<! )/) do |token|
      query.scan(/\w+|\W/) do |token|
        token_offset = $~.offset(0)
        case token
        when ":"
          @parsed << { value: token, delete: false, type: 'colon', offset: token_offset }
        when '"'
          @parsed << { value: token, delete: false, type: 'quote', offset: token_offset }
        when '('
          @parsed << { value: token, delete: false, type: 'open_round_bracket', offset: token_offset }
        when ')'
          @parsed << { value: token, delete: false, type: 'close_round_bracket', offset: token_offset }
        when '['
          @parsed << { value: token, delete: false, type: 'open_box_bracket', offset: token_offset }
        when ']'
          @parsed << { value: token, delete: false, type: 'close_box_bracket', offset: token_offset }
        when /OR|AND|NOT/
          @parsed << { value: token, delete: false, type: 'operator', offset: token_offset }
        else
          @parsed << { value: token, delete: false, type: 'term', offset: token_offset } # unless token.eql?(' ')
        end
      end
      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def get_not_deleted_term_index(term_index, term_step)
      look_index = term_index + (term_step)
      return term_index if look_index < 0 && (look_index < @parsed.length || !@parsed[look_index][:type].eql?('term'))
      return term_index if look_index >= @parsed.length || !@parsed[look_index][:type].eql?('term')

      term = @parsed[look_index]
      if term[:delete] || !term[:type].eql?('term')
        look_index = get_not_deleted_term_index(look_index, term_step)
      end

      look_index
    end

    def fix_tokenization
      terms = @parsed.select { |s| s[:type].eql?('term') }
      terms.each do |term|
        term[:value] = term[:value].gsub('\\', '') # cleanup
        term_index = @parsed.rindex(term)

        if term_index > 0 && !term[:delete]
          prev_term_index = get_not_deleted_term_index(term_index, -1)
          next_term_index = get_not_deleted_term_index(term_index, 1)
          prev_term = @parsed[prev_term_index]
          next_term = @parsed[next_term_index]
          #
          next if !prev_term[:type].eql?('term')
          if ['?', '-', ',', '!', '@', '#', '$', ';', ':', '/', '%', '^', '&', '"', '|', '+', '\'', "~", '`', '<', '>', '(', ')'].include?(term[:value])
            prev_term[:value] += term[:value]
            term[:delete] = true

            if !next_term[:value].blank? && next_term[:type].eql?('term') && next_term_index != term_index
              prev_term[:value] += next_term[:value]
              next_term[:delete] = true
            end
          end
        end
      end
      @parsed.delete_if { |d| d[:delete] }
      @parsed = reset_offset(@parsed)
      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def determine_indexes
      colon = @parsed.select { |s| s[:type].eql?('colon') }
      colon.each do |c|
        colon_index = @parsed.rindex(c)
        if colon_index > 0
          term_index = colon_index - 1
          term = @parsed[term_index]
          if @indexes.include?(term[:value])
            c[:delete] = true
            @parsed[term_index][:type] = 'index'
          else
            c[:type] = 'term'
          end
        end
      end
      @parsed.delete_if { |d| d[:delete] }

      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    ##
    # add indexes to the beginning of each term sequence
    # ex. t t o t o i t t t
    # normalized form
    # i t t o t o i t t t
    #
    # Operator precedence
    # NOT(must_not), AND(must), OR(should)
    #
    # TODO: Grouping ()
    #
    def normalize_indexes
      default_index = { value: "any", delete: false, type: "index", offset: [] }
      default_operator = { value: "AND", delete: false, type: "operator", offset: [] }

      qbl = query_bit_list()

      case qbl[0]
      when 't'
        @parsed.insert(0, default_index.clone)
        @parsed.insert(0, default_operator.clone)
      when 'i'
        @parsed.insert(0, default_operator.clone)
      end

      @parsed = reset_offset(@parsed)
      qbl = query_bit_list()

      # check if every index has an operator
      @parsed.select { |s| s[:type].eql?('index') }.each do |index|
        i = @parsed.rindex(index)
        unless qbl[i - 1].eql?('o')
          @parsed.insert(i, default_operator.clone)
        end
      end

      @parsed = reset_offset(@parsed)
      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def normalize_terms
      changes = 1
      while changes != 0
        changes = 0
        terms = @parsed.select { |s| s[:type].eql?('term') || s[:type] =~ /bracket/ }
        prev_term = nil
        terms.reverse.each_with_index do |term, _|
          unless prev_term.nil?
            if prev_term[:offset][0] == term[:offset][1] && !term[:type].eql?('close_round_bracket')
              #              term[:value] += ' ' unless ['.','(',')','[',']','&'].include?(term[:value]) || ['.','(',')','[',']','&'].include?(prev_term[:value][0])
              term[:value] += prev_term[:value]
              term[:type] = 'term'
              prev_term[:delete] = true
              changes += 1
            end
          end

          prev_term = term
        end

        @parsed.delete_if { |d| d[:delete] }
        @parsed = reset_offset(@parsed)
        # remove_single_space
        terms.each do |term|
          term[:value].strip!
          if term[:value] =~ /^\(/ && term[:value] =~ /\)$/
            term[:value] = term[:value].gsub(/^\(/, '').gsub(/\)$/, '')
          end
        end
      end
      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def remove_single_space
      @parsed.each_with_index do |p, i|
        prev_p = i > 0 ? @parsed[i - 1] : p
        next_p = i < p.length ? @parsed[i + 1] : p
        p[:delete] = true if (p[:value].eql?(' ') || p[:value].empty?) && (prev_p[:type].eql?('operator') || next_p[:type].eql?('operator'))
      end
      @parsed.delete_if { |d| d[:delete] }
      @parsed = reset_offset(@parsed)
      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def normalize_rounded_brackets
      until (open_round_brackets = @parsed.select { |s| s[:type].eql?('open_round_bracket') }).empty?
        start_index = @parsed.index(open_round_brackets.first)
        matching_bracket = find_closing_round_bracket(start_index)

        next_start_index = open_round_brackets.length > 1 ? @parsed.index(open_round_brackets[1]) : 0

        if matching_bracket.eql?(@parsed.length - 1) && (@parsed[start_index - 1][:type].eql?('index') || start_index == 0)
          @parsed.delete_at(matching_bracket)
          @parsed.delete_at(start_index)
        elsif start_index == 0 && matching_bracket > 0
          @parsed.delete_at(matching_bracket)
          @parsed.delete_at(start_index)
        elsif (next_start_index - start_index) == 1 &&
          open_round_brackets[0][:type] == open_round_brackets[1][:type] &&
          open_round_brackets[0][:offset][1] == open_round_brackets[1][:offset][0] # double brackets
          @parsed.delete_at(matching_bracket)
          @parsed.delete_at(start_index)
        elsif @parsed[start_index - 1][:type].eql?('index') && !@parsed[matching_bracket + 1].eql?('term')
          @parsed.delete_at(matching_bracket)
          @parsed.delete_at(start_index)
        elsif @parsed[start_index - 1][:type].eql?('operator') && !@parsed[matching_bracket + 1].eql?('term')
          @parsed.delete_at(matching_bracket)
          @parsed.delete_at(start_index)
        elsif matching_bracket.eql?(@parsed.length - 1) && !facet?(find_index_in_query_from(start_index)[:value])
          @parsed.delete_at(matching_bracket)
          @parsed.delete_at(start_index)
        else
          break
        end
      end

      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def find_index_in_query_from(index_location)
      @parsed.select{|s| s[:type].eql?('index')}.reverse.each do |index|
        current_index_location = @parsed.rindex(index)
        break index if current_index_location < index_location
      end
    end

    def find_closing_round_bracket(candidate_i_start)
      candidate_index_end = -1
      bracket_counter = 0
      @parsed[candidate_i_start..@parsed.length].each_with_index do |part, index|
        bracket_counter += 1 if part[:type].eql?('open_round_bracket')
        bracket_counter -= 1 if part[:type].eql?('close_round_bracket')

        if part[:type].eql?('close_round_bracket') && bracket_counter == 0
          candidate_index_end = candidate_i_start + index
          break
        end
      end
      candidate_index_end
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      -1
    end

    def determine_phrases
      while (quotes = @parsed.select { |s| s[:type].eql?('quote') }).length > 0
        phrase = []
        quote_index = @parsed.rindex(quotes[0])

        @parsed[quote_index..@parsed.length].each_with_index do |t, i|
          if t[:type].eql?('quote') && i > 0
            t[:delete] = true
            break
          else
            if i > 0
              phrase_term = t.clone
              phrase_term[:type] = 'term'
              phrase << phrase_term
            end
            t[:delete] = true
          end

        end

        phrase = "\"#{phrase.map { |m| m[:value] }.join}\""

        @parsed[quote_index] = { value: phrase, delete: false, type: 'term', offset: [0, 0] }
        @parsed.delete_if { |d| d[:delete] }
      end

      terms = @parsed.select { |s| s[:type].eql?('term') }
      terms.each do |t|
        if t[:value].eql?('\\')
          t[:delete] = true
        end
        t[:value] = t[:value].gsub(/\\"/, '"')
        t[:value] = t[:value].gsub(/\\:/, ':')
        t[:value] = t[:value].gsub(/\\\]/, ']')
        t[:value] = t[:value].gsub(/\\\[/, '[')
      end

      @parsed.delete_if { |d| d[:delete] }
      @parsed = reset_offset(@parsed)

      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def normalize_dates
      new_date_range = []
      date_ranges = @parsed.select { |s| s[:type].eql?('index') && s[:value] =~ /(start|end)?date/ }
      if date_ranges.length > 0
        date_ranges.each do |date_range|
          index = @parsed.rindex(date_range)
          parsed[index][:value] = 'date_range'
        end
      end

      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def normalize_operators
      operators = @parsed.select { |s| s[:type].eql?('operator') }
      not_operators = operators.select { |s| s[:value].eql?('NOT') }

      if not_operators
        not_operators.each do |not_operator|
          index = @parsed.rindex(not_operator)
          if @parsed[index - 1][:value].eql?(' ') && @parsed[index - 2][:type].eql?('operator')
            @parsed[index - 1][:delete] = true
            @parsed[index - 2][:delete] = true
          elsif @parsed[index - 1][:type].eql?('operator')
            @parsed[index - 1][:delete] = true
          end

          @parsed.delete_if { |d| d[:delete] }
          @parsed = reset_offset(@parsed)
        end
      end

      unless @parsed[0][:type].eql?('operator')
        @parsed.unshift({ :value => "AND", :delete => false, :type => "operator", :offset => [0, 0] })
        @parsed = reset_offset(@parsed)
      end

      if @parsed.last[:type].eql?('operator')
        @parsed.last[:delete] = true
        @parsed.delete_if { |d| d[:delete] }
        @parsed = reset_offset(@parsed)
      end

      self
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    private

    def filter?(index)
      index =~ /^facet_/
    end

    def process_date(elastic_index, term)
      query_terms = term
      if query_terms =~ /^\(?\[? *(-?\d{3,4}|NaN) +TO +(-?\d{3,4}|NaN) *\]?\)?$/
        from_date = $1
        to_date = $2

        from_date = '1970' unless from_date =~ /^\d+$/ && from_date.length <= 4
        to_date = '9999' unless to_date =~ /^\d+$/ && to_date.length <= 4

        from_date = "#{from_date}-01-01"
        to_date = "#{to_date}-12-31"

        fragment = { 'range' => { elastic_index => { 'gte' => from_date, 'lte' => to_date, 'relation' => @date_range_relation } } }
      elsif query_terms =~ /^\(?\[?(-?\d{8}) TO (-?\d{8})\]?\)?$/
        from_date = $1
        to_date = $2

        fragment = { 'range' => { elastic_index => { 'gte' => from_date, 'lte' => to_date, 'relation' => @date_range_relation } } }
      end

      fragment
    end

    def query_bit_list()
      bit_list = []

      @parsed.each do |p|
        case p[:type]
        when 'index'
          bit_list << 'i'
        when 'operator'
          bit_list << 'o'
        else
          bit_list << 't'
        end
      end
      bit_list
    end

    def facet?(index)
      @indexes.facet_map.key?(index) # || @indexes.include?(index) && @indexes.list[index] =~ /\.keyword$/
    end

    def elastic_operator(operator)
      case operator
      when 'OR'
        'should'
      when 'NOT'
        'must_not'
      else
        #'AND'
        'must'
      end
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      self
    end

    def reset_offset(data)
      offset_start = 0
      offset_end = 0
      data.each_with_index do |d, _|
        offset_end += d[:value].length
        d[:offset] = [offset_start, offset_end]
        offset_start = offset_end
      end

      data
    rescue StandardError => e
      log_error(e.message, __method__.to_s, __LINE__)
      data
    end

    def log_error(message, method_name, line)
      puts "<!#{line} #{method_name}"
      puts message
      pp @parsed
      puts "=========================>"
    end
  end
end
