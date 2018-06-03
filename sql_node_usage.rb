#!/usr/bin/env ruby

require 'optparse'
require 'sqlite3'

################################################################################
# Supporting code for running SQL queries against the SQLite3 database used in
# this interview so that interviewees don't have to set up a lot of
# infrastructure.
#
# THIS CODE IS NOT SAFE FOR GENERAL CONSUMPTION.
################################################################################

module NodeUsageDb
  TABLE_NAME = "usage_data"
  PATH = File.join(File.dirname(__FILE__), "..", "usage_data.sqlite3")
  DB = SQLite3::Database.new PATH
  COLUMNS = {
    :node_id => "INTEGER",
    :timestamp => "REAL",
    :kb => "INTEGER",
  }

  def self.run!(cmd)
    DB.execute(cmd)
  end

  def self.drop!
    self.run!("DROP TABLE IF EXISTS #{TABLE_NAME}")
  end

  def self.create!
    self.run!(<<-EOF)
CREATE TABLE IF NOT EXISTS #{TABLE_NAME}(#{COLUMNS.map { |k, v| "#{k} #{v}" }.join(",")})
    EOF
  end
end

def usage_and_exit!
  puts <<-EOF
Usage: #{__FILE__} <cmd> [<params>]

where cmd is one of reset, sql.

If cmd is reset, the command takes no parameters and simply resets all relevant
database state.

If cmd is sql, <params> is a SQL command to run. The command should be
surrounded by some form of quotes to ensure it is a single parameter, e.g.,

  #{__FILE__} sql "SELECT * FROM foo;"

For a query that returns a non-empty set of results, this script will print
each row on a separate line, formatted as a CSV. Otherwise, the script prints
out nothing.
  EOF
  exit -1
end

if __FILE__ == $0
  OptionParser.new do |opts|
    opts.on("-h", "--help", "Help") do |v|
      usage_and_exit!
    end
  end.parse!

  usage_and_exit! if ARGV.size == 0
  case ARGV[0]
  when "reset"
    usage_and_exit! if ARGV.size != 1
    NodeUsageDb.drop!
    NodeUsageDb.create!
  when "sql"
    usage_and_exit! if ARGV.size != 2
    puts NodeUsageDb.run!(ARGV[1]).map { |row| row.join(",") }.join("\n")
  end
end
