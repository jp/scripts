require 'fog'
require 'thread'
require 'optparse'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: s3uploader.rb [options] [directories|files]"

  opts.on('-a', '--access-key ACCESS_KEY', 'Access key') { |v| options[:aws_access_key_id] = v }
  opts.on('-s', '--secret-key SECRET_KEY', 'Secret key') { |v| options[:aws_secret_access_key] = v }
  opts.on('-b', '--bucket BUCKET', 'Target bucket') { |v| options[:bucket] = v }
  opts.on('-r', '--region REGION', 'Bucket region') { |v| options[:region] = v }

end.parse!

options[:paths] = ARGV

NUMBER_OF_THREADS = 10
BATCH_SIZE = 1000

# Fog connection

CONNECTION = Fog::Storage.new({
  :provider                 => 'AWS',
  :aws_access_key_id        => options[:aws_access_key_id],
  :aws_secret_access_key    => options[:aws_secret_access_key],
  :region                   => options[:region]
})

S3 = CONNECTION.directories.get(options[:bucket])

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

  def length
    @jobs.length
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

## Human readable filesize
def humanize_size(s)
  units = %W(B KiB MiB GiB TiB)
  size, unit = units.reduce(s.to_f) do |(fsize, _), utype|
    fsize > 512 ? [fsize / 1024, utype] : (break [fsize, utype])
  end
  "#{size > 9 || size.modulo(1) < 0.1 ? '%d' : '%.1f'} %s" % [size, unit]
end

def upload_file(s3_key, file)
  $pool.schedule do
    puts "uploading #{humanize_size(File.size(file))} - #{s3_key}"
    S3.files.create(
      :key      => s3_key,
      :body     => File.open(file),
      :public   => false,
      :metadata => { 'Content-Disposition' => 'attachment' }
      )
    puts "uploaded : "+ s3_key
  end
end

## Uploader

$i = 0

$pool = Pool.new(NUMBER_OF_THREADS)

options[:paths].each do |path|
  if !File.directory?(path)
    upload_file(Pathname.new(path).basename.to_s,path)
  else
    Dir.glob(path+"/**/*").each do |file|
      if !File.directory?(file) && !File.symlink?(file)
        s3_filename = file.gsub(path+'/',"")
        $i += 1

        upload_file(s3_filename,file)

        if $i > BATCH_SIZE
          $i = 0
          while $pool.length > 0
            sleep 1
            print '.'
          end
        end

      end
    end
  end
end

at_exit { $pool.shutdown }
