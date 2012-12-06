require 'rubygems'
require 'fog'
require 'thread'

# Awesome threaded S3 uploader
# -------------------------

# Thanks to Kim Burgestrand
# http://burgestrand.se/articles/quick-and-simple-ruby-thread-pool.html

NUMBER_OF_THREADS = 10

# directory to upload
path = "/path/to/upload/"

# Fog connection

CONNECTION = Fog::Storage.new({
  :provider                 => 'AWS',
  :aws_access_key_id        => "XXXXX",
  :aws_secret_access_key    => "XXXXX",
  :region                => "us-west-2"
  })

S3 = CONNECTION.directories.get("bucket-name")

# Awesome thread pool by Kim Burgestrand

class Pool
  def initialize(size)
    @size = size
    @jobs = Queue.new
    
    @pool = Array.new(@size) do |i|
      Thread.new do
        Thread.current[:id] = i
        catch(:exit) do
          loop do
            job, args = @jobs.pop
            job.call(*args)
          end
        end
      end
    end
  end

  def schedule(*args, &block)
    @jobs << [block, args]
  end

  def shutdown
    @size.times do
      schedule { throw :exit }
    end
    @pool.map(&:join)
  end
end



$i = 0
$j = 0

p = Pool.new(NUMBER_OF_THREADS)

Dir.glob(path+"/**/*").each do |file|
	if !File.directory?(file) && !File.symlink?(file)
		s3_filename = file.gsub(path+'/',"")
    $j += 1
    puts "#{$j} : "+ s3_filename


    p.schedule do
     $i += 1
     S3.files.create(
      :key    => s3_filename,
      :body   => File.open(file)
      )
     puts "#{$i} : "+ file
   end
 end
end

at_exit { p.shutdown }
