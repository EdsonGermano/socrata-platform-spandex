require 'uri'
require 'openssl'
require 'net/http'
require 'json'

class DatasetModel

  $default_rows_count = 8000000
  $default_fake_datatype = [UniqueColumn,SmallVariability,MediumVariability]

  def initialize(domain, uid, column_count)
    #Add two alpha numeric characters to match the length                                                                                                            
    @domain = domain
    @uid = uid
    @columns = generate_fake_column_uids(column_count.to_i)
    @generators = get_datagenerators
    @working_copy = 1
  end

  def generate_fake_column_uids(column_count)
    uids = Array.new
    (0..column_count).each do |i|
      fake_uid = (0...4).map { ('a'..'z').to_a[rand(26)] }.join + "-" + (0...4).map { ('a'..'z').to_a[rand(26)] }.join
      uids.push fake_uid
    end
    uids
  end

  # Loop through the fake data generators.  Creating one for each column type                                                                                        
  def get_datagenerators
    generators = Array.new
    (0..get_column_count).each do |i|
      generator_class = $default_fake_datatype[($default_fake_datatype.length - i) % $default_fake_datatype.length]
      generator = generator_class.new
      generators.push generator
    end
    generators
  end

  def get_column_count
    @columns.length #_count                                                                                                                                          
  end

  def get_row_count
    if @row_count.nil?
      url = URI.parse(get_count_url(@domain,@uid))
      proxy_host = "proxy.aws-us-west-2-staging.socrata.net"
      proxy_port = 3128
      http = Net::HTTP.new(url.host, 443,proxy_host,proxy_port)
      http.use_ssl = true
      path = url.path
      params = url.query

      http.verify_mode = OpenSSL::SSL::VERIFY_NONE

      headers = {
        'Authorization' => 'Basic ZnJhbmtsaW4ud2lsbGlhbXNAc29jcmF0YS5jb206UzBjcmF0YSE=',
        'X-App-Token' => 'UvJap5g1VKRTFcG4wFzEIm7RO'
      }

      resp, data = http.get("#{path}?#{params}", headers)

      if resp.kind_of? Net::HTTPSuccess then
        json = JSON.parse(resp.body)

        @row_count = json[0]["count"].to_i
      else
        @row_count = $default_rows_count
      end
    end
    @row_count
  end

  def get_count_url(domain,fourfour)
    "https://#{domain}/resource/#{fourfour}.json\?$select=count(*)"
  end

  def get_sample(col_index)
    value = @generators[col_index].get_sample
    dataset_uid = (0...2).map { ('a'..'z').to_a[rand(26)] }.join + @uid
    column_uid = @columns[col_index]
    sample = Sample.new(dataset_uid,@working_copy,column_uid,value)
    sample
  end

end

class Sample
  def initialize(uid, working_copy, column_uid, value)
    @dataset_uid = uid
    @working_copy = working_copy
    @column_uid = column_uid
    @value = value
  end

  def get_composite_id
    "#{@dataset_uid}|#{@working_copy}|#{@column_uid}"
  end

  def get_document
    document = { :dataset_id => @dataset_uid, :copy_id => @working_copy, :column_id => @column_uid, :composite_id => get_composite_id, :value => @value }
    document
  end

end
