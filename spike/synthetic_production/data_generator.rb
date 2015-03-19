#!/usr/bin/env ruby

class DataGenerator

  #@values = Array.new
  #@current_index = 0

  def initialize(string_length, unique_values = nil)
    @string_length = string_length
    @unique_values = unique_values
    @values = Array.new
    @current_index = 0
  end

  def get_sample
    if @unique_values.nil?
      #don't even bother storing it for later use
      return get_random_string
    else
      return get_cached_string
    end
  end
  
  def get_cached_string
    if @values.length.eql? @unique_values
      random = @values[(@values.length - @current_index) % @values.length]
      @current_index = @current_index + 1
      return random 
    else
      random = get_random_string
      @values.push random
      return random
    end
  end

  def get_random_string
    (0...@string_length).map { ('a'..'z').to_a[rand(26)] }.join
  end

end

class UniqueColumn < DataGenerator
  def initialize() #string_length) 
    super rand(255),nil
  end
end

class SmallVariability < DataGenerator
  def initialize() #string_length)
    super rand(255), 20
  end
end

class MediumVariability < DataGenerator
  def initialize() #string_length)
    super rand(255), 100
  end
end


class LargeVariability < DataGenerator
  def initialize() #string_length)
    super rand(255), 1000
  end
end

class PointColumn < DataGenerator

end
