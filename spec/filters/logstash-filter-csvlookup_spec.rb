require 'spec_helper'
require "logstash/filters/csvlookup"

describe LogStash::Filters::CsvLookup do
  describe "CSV Lookup filter" do
    let(:config) do <<-CONFIG
      filter {
        csvlookup {
          message => "Hello World"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject).to include("message")
      expect(subject['message']).to eq('Hello World')
    end
  end
end
