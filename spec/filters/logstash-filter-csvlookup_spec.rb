require 'spec_helper'
require "logstash/filters/example"

describe LogStash::Filters::CsvLookup do
  describe "This csvlookup filter enrich contents using a value taken from a csv file." do
    let(:config) do <<-CONFIG
      filter {
        csvlookup {
          path   = "./testfile.csv"
          source = "number"
          values = ["letter"]
          memory = true
          header = false
        }
      }
    CONFIG
    end

    sample("number" => "1") do
      expect(subject).to include("number")
      expect(subject['number']).to eq('A')
    end

  end
end
