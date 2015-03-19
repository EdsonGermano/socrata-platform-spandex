require 'elasticsearch/transport'
require 'elasticsearch/api'
require 'elasticsearch'

class ElasticWriter
  $index_name = 'spandex3'
  $host = '10.110.95.178:9200'
  $current_byte_size = 0
  $bulk_update_threshold = 15728640



  def initialize

    @client = Elasticsearch::Client.new host: $host
    puts @client.transport.connections.first.connection.builder.handlers
    response = @client.perform_request 'GET', '_cluster/health'
    setup_index
    puts response.body
    @bulk_update = Hash.new
    @documents = Array.new
  end

  def setup_index
    @client.indices.delete index: $index_name
    @client.indices.create index: $index_name, body: get_index_hash
  end

  # Repurposing Urmila's script                                                                                                                                      
  def get_index_hash
    index = { :field_value =>
      { :properties  =>
        { :dataset_id =>
          { :type => "string" },
          :copy_id => { :type => "long" },
          :column_id =>  { :type =>  "string" },
          :composite_id => { :type => "string" },
          :value => { :type => "completion", :analyzer => "keyword", :payloads => false, :preserve_separators => true, :preserve_position_increments => true, :max_input_length => 50, :context => { :composite_id => { :type => "category", :path =>  "composite_id" } } } } } }
    index
  end

  # Queue up a bunch of documents.  Once the documents reach a particular size, write them                                                                           
  def bulk_write(sample)
    document = sample.get_document
    $current_byte_size = $current_byte_size + document.to_s.bytesize
    update =  { index:  { _index: $index_name, _type: 'field_value', _id: sample.get_id, data: document } }
    @documents.push update

    #puts $current_byte_size                                                                                                                                         
    if $current_byte_size > $bulk_update_threshold
      file = File.open("/home/ubuntu/spike/writeoutput.csv","a")
      # puts "Writing " + @documents.length.to_s + " documents"                                                                                                      
      result = @client.bulk body: @documents
      # puts result.to_s[0,250]
#      puts "Wrote: " + @documents.length.to_s + "in " + result["took"].to_s + "s.  Errors? " + result["errors"].to_s                                                
      file.puts @documents.length.to_s + "," + result["took"].to_s + "," + result["errors"].to_s
      @documents.clear
      $current_byte_size = 0
    end
  end

  def write_single_row(sample) #document)                                                                                                                            
    document = sample.get_document
    puts body
    response = @client.index index: $index_name, type: "field_value", _id: sample.get_id, body: document
    puts response
  end



end
