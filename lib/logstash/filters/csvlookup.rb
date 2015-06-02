# Looks up a key in a CSV file and adds values to the event.
#
# Usage:
# filter {
#   csvlookup {
#     path   = "/etc/logstash/plugins/csvlookup.conf"
#     source = "ip"
#     values = ["subscription","entity"]
#     memory = true
#     header = false
#   }
# }

require "logstash/filters/base"
require "logstash/namespace"
require "csv"

class LogStash::Filters::CsvLookup < LogStash::Filters::Base

  config_name "csvlookup"
  milestone 1

  # Path to the CSV file to load.
  config :path, :validate => :string, :required => true

  # The field containing the key to look up in the CSV file.
  config :source, :validate => :string, :required => true

  # The names of the fields in the CSV file. These names will be used
  # as the key names to be added to the event. Lines in the CSV file
  # should have an equal number of fields + 1 (for the key). Lines with
  # more of less fields are ignored.
  config :fields, :validate => :array, :required => true

  # Prepend the specified string to the field names listed in fields.
  # Useful if you use the same field names in multiple CSV lookups.
  config :pre, :validate => :string, :default => ''

  # Load the CSV file in memory or search the file for every event that
  # is processed.
  config :memory, :validate => :boolean, :default => true

  # Specifiy if the CSV file has a header line. If true, the first line is
  # skipped.
  config :header, :validate => :boolean, :default => false

  # Specify the column seperation character. Default is ','.
  config :col_sep, :validate => :string, :default => ','

  public
  def register
    @lookup = {}
    if File.exists? @path
      # Load the database
      @logger.error("Loading CSV #{@path}.")
      i = 0
      if @memory
        CSV.foreach(@path, {:col_sep => @col_sep, :headers => @header}) do |row|
          i+=1
          if row.length >= @fields.length + 1
            key = row.shift
            @lookup[key] = row
          else
            @logger.warn "Not enough rows on line #{i} in file #{@path}. Found #{row.length} expected #{@fields.size + 1} rows."
            @logger.warn "#{row.to_s}"
          end
        end
      end
    else
      raise "csvlookup: Config file does not exist."
    end
  end

  public
  def filter(event)
    # return nothing unless there's an actual filter event
    return unless filter?(event)

    if event[@source]
      if @memory
        @logger.debug "Memory lookup"
        if @lookup.has_key? event[@source]
          values = Array.new @lookup[event[@source]]
          @fields.each do |field|
            val = values.shift
            event[pre + field] = val unless val.nil?
          end
        end
      else
        @logger.debug "CSV file lookup"
        i = 0
        CSV.foreach(@path, {:col_sep => @col_sep, :headers => @header}) do |row|
          i += 1
          if row.length == @fields.length + 1
            key = row.shift
            if key == event[@source]
              @fields.each do |field|
                val = row.shift
                event[pre + field] = val unless val.nil?
              end
            end
          else
            @logger.warn "Not enough rows on line #{i} in file #{@path}. Found #{row.length} expected #{@fields.size + 1} rows."
            @logger.warn "#{row.to_s}"
          end
        end
      end
    end

    # filter_matched should go in the last line of our successful code 
    filter_matched(event)
  end
end
