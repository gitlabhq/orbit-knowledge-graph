
module Service
  class Runner
    def execute
      puts "Executing..."
    end
  end
end

module Client
    def self.build
        Service::Runner.new
    end
end

runner_instance = Client.build
runner_instance.execute # Reference 1: Should trace to Service::Runner#execute

raw_runner = Service::Runner.new
raw_runner.execute      # Reference 2: Should trace to Service::Runner#execute

Service::Runner.new.execute # Reference 3: Direct call, should resolve to Service::Runner#execute

BUILDER = Client
builder_runner = BUILDER.build
builder_runner.execute # Reference 4: Should trace through constant to Service::Runner#execute

$global_runner = Service::Runner.new
$global_runner.execute # Reference 5: Should trace global var to Service::Runner#execute

# Untraceable / Direct
puts "hello" # Reference 6: Built-in, likely no FQN or just 'puts'
Math.sqrt(4) # Reference 7: Standard library, should resolve to Math::sqrt
