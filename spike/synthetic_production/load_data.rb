#!/usr/bin/env ruby

require_relative 'data_generator'
require_relative 'elastic_writer'
require_relative 'dataset_model'
require 'csv'

file = File.open("/home/ubuntu/spike/textcolumns.csv","r")

writer = ElasticWriter.new

total_additions = 0
goal = 23000000000
while total_additions < goal do
  puts "Reading from file"
  file.each_line do |line|
    row = line.parse_csv
    domain = row[0]
    uid = row[1]
    column_count = row[2]

    model = DatasetModel.new(domain,uid,column_count)

    (0..model.get_column_count).each do |i|
      (0..model.get_row_count).each do |j|
        if total_additions % 1000000 == 0
          STDOUT.write "."
        end
        total_additions = total_additions + 1
        writer.bulk_write model.get_sample(i)
      end
    end
  end
end
