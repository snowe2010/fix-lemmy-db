# frozen_string_literal: true

require 'pg'
require 'net/ssh/gateway'
require 'csv'
require 'benchmark'

DRY_RUN = false

# SSH tunnel configuration
SSH_HOST = 'ip'
SSH_USER = 'root'
SSH_PORT = 22

# PostgreSQL configuration
PG_HOST = 'localhost' # Database host on remote server
PG_PORT = 4321
# PG_DATABASE = 'lemmy'
# PG_USER = 'lemmy'
# PG_PASSWORD = 'password'
PG_DATABASE = 'postgres'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'
DUPLICATES_FILE = "original_duplicates.csv"

# Create SSH tunnel
# gateway = Net::SSH::Gateway.new(
#   SSH_HOST,
#   SSH_USER,
#   port: SSH_PORT,
#   keys: ["/Users/tyler/.ssh/..."],
# )

# Forward local port to remote PostgreSQL port
# local_port = gateway.open(PG_HOST, PG_PORT)

@conn = nil

def log(table, method, is_start)
  dryrun = DRY_RUN ? "Is Dry Run" : ""
  string = "|| %10s | %s | %-s ||" % [dryrun, table.center(20), method]
  puts if is_start
  puts "=" * string.length if is_start
  puts string
  puts "=" * string.length unless is_start
  puts unless is_start
end

def is_duplicate_community?(transaction, id1, id2)
  log("community", "is_duplicate_community?", true)
  is_duplicate = false
  transaction.exec("select * from community where id = '#{id1}' or id = '#{id2}';") do |result|
    puts "      ID | name             | title"
    result.each do |row|
      puts " %7d | %-16s | %s " %
             row.values_at('id', 'name', 'title')
    end
    is_duplicate = true if result.ntuples > 1
  end
  log("community", "is_duplicate_community?", false)
  is_duplicate
end

# @param unique_on decides what columns to use to check uniqueness
def compare_and_merge_records(transaction, table, original_id, duplicate_id, unique_on = nil)
  log(table, "compare_and_merge_records", true)
  # Get all records for both IDs
  rows_id1 = []
  rows_id2 = []

  transaction.exec("select * from #{table} where community_id = '#{original_id}'") do |result|
    rows_id1 = result.to_a
  end

  transaction.exec("select * from #{table} where community_id = '#{duplicate_id}'") do |result|
    rows_id2 = result.to_a
  end
  puts "DUPLICATES:"
  p rows_id2

  # Create comparison keys for each row (excluding the community_id column)
  id1_keys = rows_id1.map { |row| row.except('community_id').except('id').values }
  id2_keys = rows_id2.map { |row| row.except('community_id').except('id').values }

  # Find duplicates (rows that appear in both sets)
  duplicates = if unique_on.nil?
                 rows_id2.select { |row2| id1_keys.include?(row2.except('community_id').except('id').values) }
               else
                 one = rows_id1.map { |row| row.fetch_values(*unique_on) }
                 rows_id2.select do |row2|
                   one.include? row2.fetch_values(*unique_on)
                 end
               end

  # Find unique rows from duplicate_id (rows that only appear in duplicate_id)
  unique_rows = if unique_on.nil?
                  rows_id2.reject { |row2| id1_keys.include?(row2.except('community_id').except('id').values) }
                else
                  one = rows_id1.map { |row| row.fetch_values(*unique_on) }
                  rows_id2.reject do |row2|
                    one.include? row2.fetch_values(*unique_on)
                  end
                end

  # Delete duplicates with duplicate_id
  if duplicates.any?
    duplicate_conditions = duplicates.map do |row|
      comparison = row.except('id').except('body').map do |col, val|
        if val.nil?
          "#{col} IS NULL"
        else
          # Escape special characters including newlines
          escaped_val = PG::Connection.escape_string(val.to_s)
          "#{col} = E'#{escaped_val}'"
        end
      end.join(' AND ')
      "(#{comparison})"
    end.join(' OR ')

    p "DELETE FROM #{table} WHERE community_id = '#{duplicate_id}' AND (#{duplicate_conditions})"
    unless DRY_RUN
      transaction.exec("DELETE FROM #{table} WHERE community_id = '#{duplicate_id}' AND (#{duplicate_conditions})") do |results|
        p results
      end
    end
  end

  # Update unique rows from duplicate_id to original_id
  if unique_rows.any?
    unique_rows.each do |row|
      conditions = row.except('id').except('body').map do |col, val|
        if val.nil?
          "#{col} IS NULL"
        else
          escaped_val = PG::Connection.escape_string(val.to_s)
          "#{col} = E'#{escaped_val}'"
        end
      end.join(' AND ')

      p "UPDATE #{table} SET community_id = '#{original_id}' WHERE community_id = '#{duplicate_id}' AND #{conditions}"
      unless DRY_RUN
        transaction.exec("UPDATE #{table} SET community_id = '#{original_id}' WHERE community_id = '#{duplicate_id}' AND #{conditions}") do |results|
          p results
        end
      end
    end
  end

  log(table, "compare_and_merge_records", false)

  {
    duplicates_deleted: duplicates.length,
    rows_updated: unique_rows.length
  }
end

# for using on aggregate tables that haven't updated
# accepts original id to reduce chance of programming error, but does nothing with it
def delete_records_from_duplicate_id(transaction, table, original_id, duplicate_id)
  log(table, "delete_records_from_duplicate_id", true)

  puts "DELETE FROM #{table} WHERE community_id = '#{duplicate_id}'"
  unless DRY_RUN
    transaction.exec("DELETE FROM #{table} WHERE community_id = '#{duplicate_id}'") do |results|
      p results
    end
  end

  log(table, "delete_records_from_duplicate_id", false)
end

# Fix up the original community and delete the duplicate
def final_community_cleanup(transaction, original_id, duplicate_id)
  log("final cleanup", "final_community_cleanup", true)
  most_recent_community = nil
  transaction.exec("select * from community where id = '#{original_id}' or id = '#{duplicate_id}';") do |result|
    most_recent_community = result.max { |row1, row2| row1.values_at('last_refreshed_at')[0] <=> row2.values_at('last_refreshed_at')[0] }
  end
  puts "most recent community id: #{most_recent_community['id']}"
  target_id = [original_id.to_i, duplicate_id.to_i].min
  duplicate_id = [original_id.to_i, duplicate_id.to_i].max
  puts "target id #{target_id}"

  # Remove 'id' and create SET clause directly from values
  set_clause = most_recent_community.reject { |k, _| k == 'id' || k == 'description' }
                                    .map do |col, val|
    if val.nil?
      "#{col} = NULL"
    else
      escaped_val = PG::Connection.escape_string(val.to_s)
      "#{col} = E'#{escaped_val}'"
    end
  end
                                    .join(', ')

  p "Finding the posts that might not have been deleted"
  p "SELECT * FROM post WHERE community_id = #{target_id}"
  p "UPDATE community SET #{set_clause} WHERE id = #{target_id}"
  p "DELETE FROM community WHERE id = #{most_recent_community['id']}"
  unless DRY_RUN
    transaction.exec("SELECT * FROM post WHERE community_id = #{target_id}") do |results|
      p results
    end
    p 'found them'
    transaction.exec("UPDATE community SET #{set_clause} WHERE id = #{target_id}") do |results|
      p results
    end
    transaction.exec("DELETE FROM community WHERE id = #{duplicate_id}") do |results|
      p results
    end
  end
  log("final cleanup", "final_community_cleanup", false)
end

def fix_posts(transaction, original_id, duplicate_id)
  log("post", "fix_posts", true)
  puts "Original id: #{original_id} duplicate id: #{duplicate_id}"
  if (original_id.to_i > duplicate_id.to_i)
    puts "THIS WAS INCORRECT... FIX IT"
  end
  log("post", "fix_posts", false)
end

DROP_CONSTRAINTS_SQL = <<-SQL
  -- Disable foreign key checks temporarily
  SET CONSTRAINTS ALL DEFERRED;
  
  -- Drop foreign key constraints
  ALTER TABLE community DROP CONSTRAINT IF EXISTS community_instance_id_fkey CASCADE;
  ALTER TABLE post DROP CONSTRAINT IF EXISTS post_community_id_fkey CASCADE;
  ALTER TABLE post DROP CONSTRAINT IF EXISTS post_creator_id_fkey CASCADE;
  ALTER TABLE post DROP CONSTRAINT IF EXISTS post_language_id_fkey CASCADE;
  
  -- Drop unique constraints
  ALTER TABLE community DROP CONSTRAINT IF EXISTS community_featured_url_key CASCADE;
  ALTER TABLE community DROP CONSTRAINT IF EXISTS community_moderators_url_key CASCADE;
  ALTER TABLE community DROP CONSTRAINT IF EXISTS idx_community_actor_id CASCADE;
  ALTER TABLE community DROP CONSTRAINT IF EXISTS idx_community_followers_url CASCADE;
  
  -- Drop primary key constraints
  ALTER TABLE community DROP CONSTRAINT IF EXISTS community_pkey CASCADE;
  ALTER TABLE post DROP CONSTRAINT IF EXISTS post_pkey CASCADE;
  
  -- Drop unique index on post
  ALTER TABLE post DROP CONSTRAINT IF EXISTS idx_post_ap_id CASCADE;

SQL

DROP_INDEXES_SQL = <<-SQL
  -- community indexes
  DROP INDEX IF EXISTS community_featured_url_key;
  DROP INDEX IF EXISTS community_moderators_url_key;
  DROP INDEX IF EXISTS community_pkey;
  DROP INDEX IF EXISTS idx_community_actor_id;
  DROP INDEX IF EXISTS idx_community_followers_url;
  DROP INDEX IF EXISTS idx_community_lower_actor_id;
  DROP INDEX IF EXISTS idx_community_lower_name;
  DROP INDEX IF EXISTS idx_community_published;
  DROP INDEX IF EXISTS idx_community_title;
  DROP INDEX IF EXISTS idx_community_trigram;
  
  -- post indexes
  DROP INDEX IF EXISTS idx_post_ap_id;
  DROP INDEX IF EXISTS idx_post_community;
  DROP INDEX IF EXISTS idx_post_creator;
  DROP INDEX IF EXISTS idx_post_language;
  DROP INDEX IF EXISTS idx_post_trigram;
  DROP INDEX IF EXISTS idx_post_url;
  DROP INDEX IF EXISTS post_pkey;


  CREATE UNIQUE INDEX community_pkey ON public.community USING btree (id);
  CREATE UNIQUE INDEX post_pkey ON public.post USING btree (id);
  CREATE INDEX idx_post_community ON public.post USING btree (community_id);
SQL

RECREATE_ALL_SQL = <<-SQL
  -- Drop primary key constraints
  ALTER TABLE community DROP CONSTRAINT IF EXISTS community_pkey CASCADE;
  ALTER TABLE post DROP CONSTRAINT IF EXISTS post_pkey CASCADE;
  
  -- Drop unique index on post
  ALTER TABLE post DROP CONSTRAINT IF EXISTS idx_post_ap_id CASCADE;

  -- First recreate primary keys
--   ALTER TABLE community ADD CONSTRAINT community_pkey PRIMARY KEY (id);
--   ALTER TABLE post ADD CONSTRAINT post_pkey PRIMARY KEY (id);
SQL
RECREATE_ALL_SQL_2 = <<-SQL
  -- Recreate unique constraints
  ALTER TABLE community ADD CONSTRAINT community_featured_url_key UNIQUE (featured_url);
  ALTER TABLE community ADD CONSTRAINT community_moderators_url_key UNIQUE (moderators_url);
  ALTER TABLE community ADD CONSTRAINT idx_community_actor_id UNIQUE (actor_id);
  ALTER TABLE community ADD CONSTRAINT idx_community_followers_url UNIQUE (followers_url);
  ALTER TABLE post ADD CONSTRAINT idx_post_ap_id UNIQUE (ap_id);
  
  -- Then recreate foreign keys with their exact definitions
  ALTER TABLE community
    ADD CONSTRAINT community_instance_id_fkey 
    FOREIGN KEY (instance_id) 
    REFERENCES instance(id) 
    ON UPDATE CASCADE 
    ON DELETE CASCADE;

  ALTER TABLE post 
    ADD CONSTRAINT post_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id)
    ON UPDATE CASCADE 
    ON DELETE CASCADE;
    
  ALTER TABLE post 
    ADD CONSTRAINT post_creator_id_fkey 
    FOREIGN KEY (creator_id) 
    REFERENCES person(id)
    ON UPDATE CASCADE 
    ON DELETE CASCADE;
    
  ALTER TABLE post 
    ADD CONSTRAINT post_language_id_fkey 
    FOREIGN KEY (language_id) 
    REFERENCES language(id);
SQL
RECREATE_ALL_SQL_3 = <<-SQL
  -- Recreate community indexes
  CREATE UNIQUE INDEX community_featured_url_key ON public.community USING btree (featured_url);
  CREATE UNIQUE INDEX community_moderators_url_key ON public.community USING btree (moderators_url);
  CREATE UNIQUE INDEX community_pkey ON public.community USING btree (id);
  CREATE UNIQUE INDEX idx_community_actor_id ON public.community USING btree (actor_id);
  CREATE UNIQUE INDEX idx_community_followers_url ON public.community USING btree (followers_url);
  CREATE UNIQUE INDEX idx_community_lower_actor_id ON public.community USING btree (lower((actor_id)::text));
  CREATE INDEX idx_community_lower_name ON public.community USING btree (lower((name)::text));
  CREATE INDEX idx_community_published ON public.community USING btree (published DESC);
  CREATE INDEX idx_community_title ON public.community USING btree (title);
  CREATE INDEX idx_community_trigram ON public.community USING gin (name gin_trgm_ops, title gin_trgm_ops);
SQL
RECREATE_ALL_SQL_4 = <<-SQL
  -- Recreate post indexes
  CREATE UNIQUE INDEX idx_post_ap_id ON public.post USING btree (ap_id);
  CREATE INDEX idx_post_community ON public.post USING btree (community_id);
  CREATE INDEX idx_post_creator ON public.post USING btree (creator_id);
  CREATE INDEX idx_post_language ON public.post USING btree (language_id);
  CREATE INDEX idx_post_trigram ON public.post USING gin (name gin_trgm_ops, body gin_trgm_ops);
  CREATE INDEX idx_post_url ON public.post USING btree (url);
  CREATE UNIQUE INDEX post_pkey ON public.post USING btree (id);
SQL
RECREATE_ALL_SQL_5 = <<-SQL
  -- First recreate the community-referenced foreign keys
  ALTER TABLE admin_purge_post 
    ADD CONSTRAINT admin_purge_post_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE community_aggregates 
    ADD CONSTRAINT community_aggregates_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE community_block 
    ADD CONSTRAINT community_block_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE community_follower 
    ADD CONSTRAINT community_follower_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE community_language 
    ADD CONSTRAINT community_language_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE community_moderator 
    ADD CONSTRAINT community_moderator_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE community_person_ban 
    ADD CONSTRAINT community_person_ban_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE mod_add_community 
    ADD CONSTRAINT mod_add_community_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE mod_ban_from_community 
    ADD CONSTRAINT mod_ban_from_community_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE mod_hide_community 
    ADD CONSTRAINT mod_hide_community_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE mod_remove_community 
    ADD CONSTRAINT mod_remove_community_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE mod_transfer_community 
    ADD CONSTRAINT mod_transfer_community_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  ALTER TABLE post_aggregates 
    ADD CONSTRAINT post_aggregates_community_id_fkey 
    FOREIGN KEY (community_id) 
    REFERENCES community(id);
  
  -- Then recreate the post-referenced foreign keys
  ALTER TABLE admin_purge_comment 
    ADD CONSTRAINT admin_purge_comment_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE comment_like 
    ADD CONSTRAINT comment_like_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE comment 
    ADD CONSTRAINT comment_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE mod_lock_post 
    ADD CONSTRAINT mod_lock_post_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE mod_remove_post 
    ADD CONSTRAINT mod_remove_post_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE mod_feature_post 
    ADD CONSTRAINT mod_sticky_post_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE person_post_aggregates 
    ADD CONSTRAINT person_post_aggregates_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE post_aggregates 
    ADD CONSTRAINT post_aggregates_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE post_like 
    ADD CONSTRAINT post_like_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE post_read 
    ADD CONSTRAINT post_read_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE post_report 
    ADD CONSTRAINT post_report_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE post_saved 
    ADD CONSTRAINT post_saved_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
  
  ALTER TABLE post_hide 
    ADD CONSTRAINT post_hide_post_id_fkey 
    FOREIGN KEY (post_id) 
    REFERENCES post(id);
SQL
RECREATE_ALL_SQL_6 = <<-SQL
  -- Re-enable constraint checking
  SET CONSTRAINTS ALL IMMEDIATE;
SQL

#### DO IT
begin
  # Connect to PostgreSQL through the tunnel
  # @conn = PG.connect(
  #   host: 'localhost',
  #   port: local_port,
  #   dbname: PG_DATABASE,
  #   user: PG_USER,
  #   password: PG_PASSWORD
  # )
  @conn = PG.connect(
    host: '127.0.0.1',
    port: 5432,
    dbname: 'lemmy',
    user: 'lemmy',
    password: 'redacted'
  )

  # Example query
  result = @conn.exec('SELECT version()')
  puts result[0]['version']

  grouped = CSV.readlines(DUPLICATES_FILE, headers: false)
               .reject { |row| row[0] == 'fixed' }
               .group_by { |row| row[10] }
               .map { |k, v| v.map { |arr| arr[1].to_i } }

  # @conn.exec("ALTER TABLE community DISABLE TRIGGER ALL;")
  # @conn.exec("ALTER TABLE post DISABLE TRIGGER ALL")
  # puts "here"
  # @conn.exec(DROP_CONSTRAINTS_SQL)
  # puts "here2"
  # @conn.exec(DROP_INDEXES_SQL)
  # puts "here3"

  total_number_of_duplicate_rows = grouped.size
  grouped.each_with_index do |ids, index|
    puts
    puts
    puts "##########################"
    puts "##########################"
    puts "##########################"
    puts "##########################"
    puts "Working on row #{index} of #{total_number_of_duplicate_rows}"
    original_id = ids.min
    duplicate_id = ids.max
    puts "original id #{original_id} : duplicate id #{duplicate_id}"
    time = Benchmark.realtime do
      duplicate_community_ = false
      @conn.transaction do |tr|
        duplicate_community_ = is_duplicate_community?(tr, original_id, duplicate_id)
      end
      puts "duplicate community? #{duplicate_community_}"
      if duplicate_community_
        @conn.transaction do |tr|
          compare_and_merge_records(tr, "admin_purge_post", original_id, duplicate_id, ["admin_person_id"])
          compare_and_merge_records(tr, "community_block", original_id, duplicate_id, ["person_id"])
          compare_and_merge_records(tr, "community_follower", original_id, duplicate_id, ["person_id"])
          compare_and_merge_records(tr, "community_language", original_id, duplicate_id, ["language_id"])
          compare_and_merge_records(tr, "community_moderator", original_id, duplicate_id, ["person_id"])
        end
        @conn.transaction do |tr|
          compare_and_merge_records(tr, "community_person_ban", original_id, duplicate_id, ["person_id"])
          compare_and_merge_records(tr, "mod_add_community", original_id, duplicate_id, ["mod_person_id", "other_person_id"])
          compare_and_merge_records(tr, "mod_ban_from_community", original_id, duplicate_id, ["mod_person_id", "other_person_id"])
          compare_and_merge_records(tr, "mod_hide_community", original_id, duplicate_id, ["mod_person_id"])
          compare_and_merge_records(tr, "mod_remove_community", original_id, duplicate_id, ["mod_person_id"])
          compare_and_merge_records(tr, "mod_transfer_community", original_id, duplicate_id, ["mod_person_id", "other_person_id"])
          compare_and_merge_records(tr, "post", original_id, duplicate_id, ["ap_id"])
        end
        @conn.transaction do |tr|
          fix_posts(tr, original_id, duplicate_id)
        end
        @conn.transaction do |tr|
          delete_records_from_duplicate_id(tr, "community_aggregates", original_id, duplicate_id)
        end
        @conn.transaction do |tr|
          delete_records_from_duplicate_id(tr, "post_aggregates", original_id, duplicate_id)
        end
        @conn.transaction do |tr|
          final_community_cleanup(tr, original_id, duplicate_id)
        end
      end
    end
    puts
    puts "Row #{index} took #{time}"
    puts "##########################"
    puts "##########################"
    puts "##########################"
    puts "##########################"
  end

  # Recreate everything if transaction succeeded
  # puts "1"
  # @conn.exec(RECREATE_ALL_SQL)
  # puts "2"
  # @conn.exec(RECREATE_ALL_SQL_2)
  # puts "3"
  # @conn.exec(RECREATE_ALL_SQL_3)
  # puts "4"
  # @conn.exec(RECREATE_ALL_SQL_4)
  # puts "5"
  # @conn.exec(RECREATE_ALL_SQL_5)
  # puts "6"
  # @conn.exec(RECREATE_ALL_SQL_6)
  # puts "6"

rescue PG::Error => e
  puts "Error in database: #{e.message}"
rescue Exception => e
  puts "Something bad happened #{e.message}"
ensure
  # Clean up
  @conn.exec("ALTER TABLE community ENABLE TRIGGER ALL")
  @conn.exec("ALTER TABLE post ENABLE TRIGGER ALL")
  @conn&.close
  # gateway.close(local_port)
  # gateway.shutdown!
end