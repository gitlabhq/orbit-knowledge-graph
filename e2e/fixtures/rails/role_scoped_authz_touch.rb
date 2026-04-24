ids = ENV.fetch('VULNERABILITY_IDS').split(',').map(&:to_i)

raise 'VULNERABILITY_IDS is empty' if ids.empty?

Vulnerability.where(id: ids).update_all(updated_at: Time.current)
puts "Touched vulnerabilities: #{ids.join(',')}"
