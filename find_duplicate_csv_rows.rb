require 'csv'

# Function to find duplicate lines in a CSV file
def find_duplicates(input_file, output_file)
  lines = {}

  # Read the CSV file and count the occurrences of each line
  CSV.foreach(input_file, headers: true) do |row|
    actor_id = row[9]
    p row[9]
    # lines[row] = lines.fetch(row, 0) + 1
    current_data = lines.fetch(actor_id, {
      count: 0,
      data: []
    })
    current_data[:count] += 1
    current_data[:data] += [row]
    lines[actor_id] = current_data
  end

  # Write the duplicate lines to a new CSV file
  CSV.open(output_file, 'w') do |csv|
    lines.each do |line, obj|
      next unless obj[:count] > 1

      obj[:data].each do |row|
        csv << row
      end
    end
  end
end

input_file = 'community_export_oct23.csv'
output_file = 'duplicates_export_oct23.csv'

find_duplicates(input_file, output_file)